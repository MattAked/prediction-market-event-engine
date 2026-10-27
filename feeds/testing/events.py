from dataclasses import dataclass


@dataclass(frozen=True)
class PredictedNumberEvent:
    number: int
    timestamp: float