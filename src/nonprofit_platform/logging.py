from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("stage", "job_type", "job_id", "worker_id", "metric", "value"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", use_json: bool = False) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if use_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


@dataclass(slots=True)
class MetricsRecorder:
    logger: logging.Logger = field(default_factory=lambda: get_logger("metrics"))

    def increment(self, metric: str, value: int = 1, **tags: Any) -> None:
        self.logger.info("metric.increment", extra={"metric": metric, "value": value, **tags})

    def timing(self, metric: str, value_ms: float, **tags: Any) -> None:
        self.logger.info("metric.timing", extra={"metric": metric, "value": round(value_ms, 2), **tags})

    @contextmanager
    def track(self, metric: str, **tags: Any) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            self.timing(metric, elapsed_ms, **tags)
