package mesosphere.marathon.core.appinfo

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.health.HealthCounts
import mesosphere.marathon.state.Timestamp
import sun.text.normalizer.VersionInfo

case class TaskStatsByVersion(
  maybeStartedAfterLastScaling: Option[TaskStats],
  maybeWithLatestConfig: Option[TaskStats],
  maybeWithOutdatedConfig: Option[TaskStats],
  maybeTotalSummary: Option[TaskStats]
)

object TaskStatsByVersion {
  def apply(now: Timestamp, versionInfo: VersionInfo, tasks: Iterable[MarathonTask], healthCounts: HealthCounts): TaskStatsByVersion = {
    def statsForVersion(versionTest: Long => Boolean): TaskStats = {
      TaskStats(now, versionInfo, tasks, healthCounts)
    }

    TaskStatsByVersion(
      maybeTotalSummary = TaskStats(now, versionInfo, tasks, healthCounts)
    )
  }
}

case class TaskLifeTime(
  averageSeconds: Double,
  medianSeconds: Double
)

object TaskLifeTime {
  def forSomeTasks(now: Timestamp, tasks: Iterable[MarathonTask]): Option[TaskLifeTime] = {
    if (tasks.isEmpty) {
      None
    } else {
      def lifeTime(task: MarathonTask): Option[Double] = {
        if (task.hasStagedAt) {
          Some((now.toDateTime.getMillis - task.getStagedAt) / 1000.0)
        } else {
          None
        }
      }

      val lifeTimes = tasks.flatMap(lifeTime).toVector.sorted
      Some(
        TaskLifeTime(
          averageSeconds = lifeTimes.sum / lifeTimes.size,
          medianSeconds = lifeTimes(lifeTimes.size / 2)
        )
      )
    }
  }
}

case class TaskStats(
  counts: TaskCounts,
  lifeTime: TaskLifeTime
)

object TaskStats {
  def apply(now: Timestamp, tasks: Iterable[MarathonTask], healthCounts: HealthCounts): Option[TaskStats] = {
    TaskLifeTime.forSomeTasks(now, tasks).map { lifeTime =>
      TaskStats(
        counts = TaskCounts(tasks, healthCounts),
        lifeTime = lifeTime
      )
    }
  }
}