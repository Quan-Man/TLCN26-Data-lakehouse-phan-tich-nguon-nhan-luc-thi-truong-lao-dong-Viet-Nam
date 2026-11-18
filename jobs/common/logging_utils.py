"""Log dùng được trong Airflow và khi chạy Python độc lập."""
from contextlib import contextmanager
import logging
import time


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    # Giữ handler của Airflow; không thêm handler lặp lại.
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


@contextmanager
def log_step(logger, name: str):
    start = time.perf_counter()
    logger.info("Bắt đầu: %s", name)
    try:
        yield
    except Exception:
        logger.exception("Thất bại: %s; %.2fs", name, time.perf_counter() - start)
        raise  # Airflow phải thấy task failed để retry.
    else:
        logger.info("Hoàn tất: %s; %.2fs", name, time.perf_counter() - start)
