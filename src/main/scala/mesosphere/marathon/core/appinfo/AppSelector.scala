package mesosphere.marathon.core.appinfo

import mesosphere.marathon.state.AppDefinition

trait AppSelector {
  def matches(app: AppDefinition): Boolean
}

object AppSelector {
  def apply(matches: AppDefinition => Boolean): AppSelector = new AppSelector {
    override def matches(app: AppDefinition): Boolean = matches(app)
  }

  def forall(selectors: Iterable[AppSelector]): AppSelector = AllMustMatch(selectors)

  private[this] case class AllMustMatch(selectors: Iterable[AppSelector]) extends AppSelector {
    override def matches(app: AppDefinition): Boolean = selectors.forall(_.matches(app))
  }
}