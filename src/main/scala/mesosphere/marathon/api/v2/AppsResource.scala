package mesosphere.marathon.api.v2

import java.net.URI
import java.util
import java.util.UUID
import javax.inject.{ Inject, Named }
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.event.EventStream
import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.v2.json.{ V2AppDefinition, V2AppUpdate }
import mesosphere.marathon.api.{ MarathonMediaType, RestResource }
import mesosphere.marathon.core.appinfo.AppInfo.Embed
import mesosphere.marathon.core.appinfo.{ AppInfo, AppInfoService, AppSelector }
import mesosphere.marathon.event.{ ApiPostEvent, EventModule }
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.upgrade.{ DeploymentPlan, DeploymentStep, RestartApplication }
import mesosphere.marathon.{ ConflictingChangeException, MarathonConf, MarathonSchedulerService, UnknownAppException }
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.Future

@Path("v2/apps")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class AppsResource @Inject() (
    @Named(EventModule.busName) eventBus: EventStream,
    appTasksRes: AppTasksResource,
    service: MarathonSchedulerService,
    appInfoService: AppInfoService,
    val config: MarathonConf,
    groupManager: GroupManager) extends RestResource {

  import mesosphere.util.ThreadPoolContext.context

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val ListApps = """^((?:.+/)|)\*$""".r

  private[this] val EmbedAppsPrefix: String = "apps."
  private[this] val EmbedAppPrefix: String = "app."
  private[this] val EmbedTasks = "tasks"
  private[this] val EmbedTasksAndFailures = "failures"

  @GET
  @Timed
  def index(@QueryParam("cmd") cmd: String,
            @QueryParam("id") id: String,
            @QueryParam("label") label: String,
            @QueryParam("embed") embed: java.util.Set[String]): String = {
    val selector = search(Option(cmd), Option(id), Option(label))
    val resolvedEmbed = resolveEmbed(embed)
    val mapped = result(appInfoService.queryAll(selector, resolvedEmbed))

    jsonObjString("apps" -> mapped)
  }

  /**
    * Converts embed arguments to our internal representation.
    *
    * Accepts the arguments with the "apps." prefix, the "app." prefix or no prefix
    * to avoid subtle user errors confusing the two.
    */
  private[this] def resolveEmbed(embed: util.Set[String]): Set[Embed] = {
    def mapEmbedStrings(embed: String): Set[AppInfo.Embed] = embed match {
      case EmbedTasks            => Set(AppInfo.Embed.Tasks, AppInfo.Embed.Deployments)
      case EmbedTasksAndFailures => Set(AppInfo.Embed.Tasks, AppInfo.Embed.LastTaskFailure, AppInfo.Embed.Deployments)
      case unknown: String =>
        log.warn("unknown embed argument: '{}' (without prefix)", unknown)
        Set.empty
    }
    def removePrefix(embedMe: String): String = {
      if (embedMe.startsWith(EmbedAppsPrefix)) {
        embedMe.substring(EmbedAppsPrefix.length)
      }
      else if (embedMe.startsWith(EmbedAppPrefix)) {
        embedMe.substring(EmbedAppPrefix.length)
      }
      else {
        embedMe
      }
    }

    val embedWithoutPrefix = embed.asScala.map(removePrefix)
    embedWithoutPrefix.flatMap(mapEmbedStrings).toSet ++ Set(AppInfo.Embed.Counts)
  }

  @POST
  @Timed
  def create(@Context req: HttpServletRequest, body: Array[Byte],
             @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {

    val app = validateApp(Json.parse(body).as[V2AppDefinition].withCanonizedIds().toAppDefinition)

    def createOrThrow(opt: Option[AppDefinition]) = opt
      .map(_ => throw new ConflictingChangeException(s"An app with id [${app.id}] already exists."))
      .getOrElse(app)

    val plan = result(groupManager.updateApp(app.id, createOrThrow, app.version, force))

    val appWithDeployments = AppInfo(
      app,
      maybeTasks = Some(Seq.empty),
      maybeDeployments = Some(Seq(Identifiable(plan.id)))
    )

    maybePostEvent(req, appWithDeployments.app)
    Response
      .created(new URI(app.id.toString))
      .entity(jsonString(appWithDeployments))
      .build()
  }

  @GET
  @Path("""{id:.+}""")
  @Timed
  def show(@PathParam("id") id: String, @QueryParam("embed") embed: java.util.Set[String]): Response = {
    val resolvedEmbed = resolveEmbed(embed) ++ Set(
      AppInfo.Embed.Tasks, AppInfo.Embed.LastTaskFailure, AppInfo.Embed.Deployments
    )
    def transitiveApps(gid: PathId): Response = {
      val withTasks = result(appInfoService.queryAllInGroup(gid, resolvedEmbed))
      ok(jsonObjString("*" -> withTasks))
    }
    def app(): Future[Response] = {
      val maybeAppInfo = appInfoService.queryForAppId(id.toRootPath, resolvedEmbed)

      maybeAppInfo.map {
        case Some(appInfo) => ok(jsonObjString("app" -> appInfo))
        case None          => unknownApp(id.toRootPath)
      }
    }
    id match {
      case ListApps(gid) => transitiveApps(gid.toRootPath)
      case _             => result(app())
    }
  }

  @PUT
  @Path("""{id:.+}""")
  @Timed
  def replace(@Context req: HttpServletRequest,
              @PathParam("id") id: String,
              @DefaultValue("false")@QueryParam("force") force: Boolean,
              body: Array[Byte]): Response = {
    val appId = id.toRootPath
    val appUpdate = Json.parse(body).as[V2AppUpdate].copy(id = Some(appId))
    BeanValidation.requireValid(ModelValidation.checkUpdate(appUpdate, needsId = false))

    val newVersion = Timestamp.now()
    val plan = result(groupManager.updateApp(appId, updateOrCreate(appId, _, appUpdate, newVersion), newVersion, force))

    val response = plan.original.app(appId).map(_ => Response.ok()).getOrElse(Response.created(new URI(appId.toString)))
    maybePostEvent(req, plan.target.app(appId).get)
    deploymentResult(plan, response)
  }

  @PUT
  @Timed
  def replaceMultiple(@DefaultValue("false")@QueryParam("force") force: Boolean, body: Array[Byte]): Response = {
    val updates = Json.parse(body).as[Seq[V2AppUpdate]].map(_.withCanonizedIds())
    BeanValidation.requireValid(ModelValidation.checkUpdates(updates))
    val version = Timestamp.now()
    def updateGroup(root: Group): Group = updates.foldLeft(root) { (group, update) =>
      update.id match {
        case Some(id) => group.updateApp(id, updateOrCreate(id, _, update, version), version)
        case None     => group
      }
    }
    deploymentResult(result(groupManager.update(PathId.empty, updateGroup, version, force)))
  }

  @DELETE
  @Path("""{id:.+}""")
  @Timed
  def delete(@Context req: HttpServletRequest,
             @DefaultValue("true")@QueryParam("force") force: Boolean,
             @PathParam("id") id: String): Response = {
    val appId = id.toRootPath

    def deleteApp(group: Group) = group.app(appId)
      .map(_ => group.removeApplication(appId))
      .getOrElse(throw new UnknownAppException(appId))

    deploymentResult(result(groupManager.update(appId.parent, deleteApp, force = force)))
  }

  @Path("{appId:.+}/tasks")
  def appTasksResource(): AppTasksResource = appTasksRes

  @Path("{appId:.+}/versions")
  def appVersionsResource(): AppVersionsResource = new AppVersionsResource(service, config)

  @POST
  @Path("{id:.+}/restart")
  def restart(@PathParam("id") id: String,
              @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    val appId = id.toRootPath
    val newVersion = Timestamp.now()
    def setVersionOrThrow(opt: Option[AppDefinition]) = opt
      .map(_.copy(version = newVersion))
      .getOrElse(throw new UnknownAppException(appId))

    def restartApp(versionChange: DeploymentPlan): DeploymentPlan = {
      val newApp = versionChange.target.app(appId).get
      val plan = DeploymentPlan(
        UUID.randomUUID().toString,
        versionChange.original,
        versionChange.target,
        DeploymentStep(RestartApplication(newApp) :: Nil) :: Nil,
        Timestamp.now())
      result(service.deploy(plan, force = force))
      plan
    }
    //this will create an empty deployment, since version chances do not trigger restarts
    val versionChange = result(groupManager.updateApp(id.toRootPath, setVersionOrThrow, newVersion, force))
    //create a restart app deployment plan manually
    deploymentResult(restartApp(versionChange))
  }

  private def updateOrCreate(appId: PathId,
                             existing: Option[AppDefinition],
                             appUpdate: V2AppUpdate,
                             newVersion: Timestamp): AppDefinition = {
    def createApp() = validateApp(appUpdate(AppDefinition(appId)))
    def updateApp(current: AppDefinition) = validateApp(appUpdate(current))
    def rollback(version: Timestamp) = service.getApp(appId, version).getOrElse(throw new UnknownAppException(appId))
    def updateOrRollback(current: AppDefinition) = appUpdate.version.map(rollback).getOrElse(updateApp(current))
    existing.map(updateOrRollback).getOrElse(createApp()).copy(version = newVersion)
  }

  private def validateApp(app: AppDefinition): AppDefinition = {
    BeanValidation.requireValid(ModelValidation.checkAppConstraints(V2AppDefinition(app), app.id.parent))
    val conflicts = ModelValidation.checkAppConflicts(app, result(groupManager.rootGroup()))
    if (conflicts.nonEmpty) throw new ConflictingChangeException(conflicts.mkString(","))
    app
  }

  private def maybePostEvent(req: HttpServletRequest, app: AppDefinition) =
    eventBus.publish(ApiPostEvent(req.getRemoteAddr, req.getRequestURI, app))

  private[v2] def search(cmd: Option[String], id: Option[String], label: Option[String]): AppSelector = {
    def containCaseInsensitive(a: String, b: String): Boolean = b.toLowerCase contains a.toLowerCase
    val selectors = Seq[Option[AppSelector]](
      cmd.map(c => AppSelector(_.cmd.exists(containCaseInsensitive(c, _)))),
      id.map(s => AppSelector(app => containCaseInsensitive(s, app.id.toString))),
      label.map(new LabelSelectorParsers().parsed)
    ).flatten
    AppSelector.forall(selectors)
  }
}
