from enum import StrEnum


class ExecutionState(StrEnum):
    PENDING_SUBMISSION = "pending_submission"
    SUBMITTED = "submitted"
    CANCELLATION_PENDING = "cancellation_pending"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    FILLED = "filled"
    REJECTED = "rejected"
    FAILED = "failed"