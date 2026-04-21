"""In-memory capture of everything the executor prints during a task run.

Both loguru (what the executor itself logs) and stdlib `logging` (what
simcore + grpc + third-party libs log) tee into the same bounded buffer.
When the task finishes, ``snapshot()`` returns the last `max_bytes` of
combined output so we can PUT it onto the task_run row and render it in
the web UI.
"""

from __future__ import annotations

import io
import logging
from loguru import logger as loguru_logger


class LogCapture:
    """Rotating in-memory buffer. Holds at most ~2x `max_bytes` before
    trimming back to the tail of `max_bytes` so we never grow unbounded on
    a long-running task."""

    def __init__(self, max_bytes: int = 512 * 1024):
        self._buf = io.StringIO()
        self._max = max_bytes

    def write(self, msg: str) -> None:
        self._buf.write(msg)
        if self._buf.tell() > self._max * 2:
            text = self._buf.getvalue()[-self._max :]
            self._buf = io.StringIO()
            self._buf.write(text)

    def snapshot(self) -> str:
        text = self._buf.getvalue()
        if len(text) <= self._max:
            return text
        # Keep the tail, prepend a marker so the truncation is obvious.
        return f"... (truncated, keeping last {self._max} bytes)\n{text[-self._max :]}"


class _StdlibToCapture(logging.Handler):
    def __init__(self, capture: LogCapture):
        super().__init__(level=logging.DEBUG)
        self._capture = capture
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._capture.write(self.format(record) + "\n")
        except Exception:
            pass


def install(capture: LogCapture) -> None:
    """Tee loguru + stdlib logging into `capture`. Stdlib root logger is
    left at whatever level the caller configured; we only append a handler.
    Loguru sink is added at DEBUG so we capture everything."""

    loguru_logger.add(
        capture.write,
        level="DEBUG",
        format="{time:HH:mm:ss} | {level: <8} | {name}:{line} - {message}\n",
    )

    root = logging.getLogger()
    # Ensure the root logger lets records through to our handler even if
    # no one configured a level yet.
    if root.level > logging.DEBUG or root.level == logging.NOTSET:
        root.setLevel(logging.DEBUG)
    root.addHandler(_StdlibToCapture(capture))
