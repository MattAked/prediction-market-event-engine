from ...strategy.base import StrategyLogic
from ...core.events import EventEnvelope
from ...core.outputs import StrategyOutput
from ...feeds.sports.sofascore.soccer.events import GoalDisallowedEvent
from .state import GoalDisallowedState


class GoalDisallowedStrategy(StrategyLogic):
    def handle_event(
        self,
        event: EventEnvelope,
        state: GoalDisallowedState,
        params: object,
    ) -> list[StrategyOutput]:
        if isinstance(event.payload, GoalDisallowedEvent):
            pass
            # Can access state here to make decisions

        return []
