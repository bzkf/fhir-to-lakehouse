import time

from loguru import logger
from opentelemetry.metrics import Histogram


class MeasureElapsed:
    def __init__(self, histogram: Histogram, attributes: dict[str, str]):
        self.histogram = histogram
        self.attributes = attributes

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = time.perf_counter() - self.start
        self.histogram.record(self.time, self.attributes)
        logger.info("Elapsed time: {time}", time=self.time)
