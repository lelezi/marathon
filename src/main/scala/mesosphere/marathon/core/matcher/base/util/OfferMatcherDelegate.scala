package mesosphere.marathon.core.matcher.base.util

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.base.OfferMatcher
import OfferMatcher.MatchedTasks
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.{ Offer, TaskInfo }

import scala.concurrent.Future

/**
  * Provides a thin wrapper around an OfferMatcher implemented as an actor.
  */
class ActorOfferMatcher(clock: Clock, actorRef: ActorRef) extends OfferMatcher {
  def matchOffer(deadline: Timestamp, offer: Offer): Future[MatchedTasks] = {
    implicit val timeout: Timeout = clock.now().until(deadline)
    val answerFuture = actorRef ? ActorOfferMatcher.MatchOffer(deadline, offer)
    answerFuture.mapTo[MatchedTasks]
  }

  override def toString: String = s"ActorOfferMatcher($actorRef)"
}

object ActorOfferMatcher {
  /**
    * Send to an offer matcher to request a match.
    *
    * This should always be replied to with a LaunchTasks message.
    */
  case class MatchOffer(matchingDeadline: Timestamp, remainingOffer: Offer)
}
