import logging
from logging.handlers import RotatingFileHandler


def get_trace_logger():
    """Configures a separate logger for LLM inputs/outputs"""
    logger = logging.getLogger("llm_trace")

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        handler = RotatingFileHandler(
            "llm_trace.log",
            mode="a",
            maxBytes=10 * 1024 * 1024,
            backupCount=2,
            encoding="utf-8",
        )

        formatter = logging.Formatter(
            "\n"
            + "=" * 80
            + "\n%(asctime)s - %(levelname)s\n"
            + "=" * 80
            + "\n%(message)s\n"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.propagate = False

    return logger
