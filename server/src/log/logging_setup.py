import sys

from loguru import logger

from common.utils.diagnostic_context import inject_diagnostic_scope


def setup_logging(log_level="INFO", log_file="knoggin.log", colorize=False):
    logger.remove()
    logger.configure(patcher=inject_diagnostic_scope)

    if colorize:
        stdout_fmt = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {extra[diagnostic_scope]} | <cyan>{module}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    else:
        stdout_fmt = "{time:YYYY-MM-DD HH:mm:ss} - {extra[diagnostic_scope]} - {file}:{line} - {level} - {message}"

    file_fmt = "{time:YYYY-MM-DD HH:mm:ss} - {extra[diagnostic_scope]} - {file}:{line} - {level} - {message}"

    logger.add(
        sys.stdout,
        format=stdout_fmt,
        level=log_level,
        colorize=colorize,
        backtrace=True,
        diagnose=True,
    )
    logger.add(
        log_file, format=file_fmt, level=log_level, rotation="2 MB", retention=10
    )

    logger.info("Logging configured successfully.")
