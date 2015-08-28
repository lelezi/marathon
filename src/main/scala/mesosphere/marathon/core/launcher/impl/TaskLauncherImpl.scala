package mesosphere.marathon.core.launcher.impl

import java.util.Collections

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.TaskLauncher
import org.apache.mesos.Protos.{ OfferID, Status, TaskInfo }
import org.apache.mesos.{ Protos, SchedulerDriver }
import org.slf4j.LoggerFactory

private[launcher] class TaskLauncherImpl(
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    clock: Clock) extends TaskLauncher {
  private[this] val log = LoggerFactory.getLogger(getClass)

  override def launchTasks(offerID: OfferID, taskInfos: Seq[TaskInfo]): Boolean = {
    withDriver(s"launchTasks($offerID)") { driver =>
      import scala.collection.JavaConverters._
      driver.launchTasks(Collections.singleton(offerID), taskInfos.asJava)
    }
  }

  override def declineOffer(offerID: OfferID, refuseSeconds: Option[Long]): Unit = {
    val filters = refuseSeconds
      .map(seconds => Protos.Filters.newBuilder().setRefuseSeconds(seconds / 1000.0).build())
      .getOrElse(Protos.Filters.getDefaultInstance)

    withDriver(s"declineOffer(${offerID.getValue})") {
      _.declineOffer(offerID, filters)
    }
  }

  private[this] def withDriver(description: => String)(block: SchedulerDriver => Status): Boolean = {
    marathonSchedulerDriverHolder.driver match {
      case Some(driver) =>
        val status = block(driver)
        if (log.isDebugEnabled) {
          log.debug(s"$description returned status = $status")
        }
        status == Status.DRIVER_RUNNING

      case None =>
        log.warn(s"Cannot execute '$description', no driver available")
        false
    }
  }
}
