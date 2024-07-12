import time
from typing import Iterable, Callable, Any


class Throttle:
    def __init__(self, delay_ms):
        self.delay_ms = delay_ms

    def execute_all(self, commands: Iterable[Callable]) -> Any:
        for command in commands:
            yield command()
            time.sleep(self.delay_ms * 1e-3)
