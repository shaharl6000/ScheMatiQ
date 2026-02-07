"""Application-level exceptions."""


class CapacityExceededError(Exception):
    """Raised when the server is at maximum concurrent session capacity."""

    def __init__(self, active_count: int, max_count: int):
        self.active_count = active_count
        self.max_count = max_count
        super().__init__(
            f"The server is currently busy processing other requests "
            f"({active_count}/{max_count} active sessions). "
            f"Please try again in a few minutes."
        )
