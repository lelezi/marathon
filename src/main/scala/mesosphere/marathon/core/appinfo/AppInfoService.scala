package mesosphere.marathon.core.appinfo

import mesosphere.marathon.state.PathId

import scala.concurrent.Future

trait AppInfoService {
  def queryForAppId(appId: PathId, embed: Set[AppInfo.Embed]): Future[Option[AppInfo]]
  def queryAllInGroup(groupId: PathId, embed: Set[AppInfo.Embed]): Future[Seq[AppInfo]]
  def queryAll(selector: AppSelector, embed: Set[AppInfo.Embed]): Future[Seq[AppInfo]]
}
