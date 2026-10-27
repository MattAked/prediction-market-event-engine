class StrategyAlreadyRegisteredError(RuntimeError):
    def __init__(self, instance_id: str):
        super().__init__(
            f"Strategy instance already registered: {instance_id}"
        )
        self.instance_id = instance_id