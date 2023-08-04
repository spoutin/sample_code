import logging
import os


logger = logging.getLogger(__name__)


def setup_logging() -> None:
    root_logger = logging.getLogger()
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    root_logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s %(name)s [%(levelname)s]: %(message)s")
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)


def main() -> None:
    setup_logging()
    logger.info("Starting create_report")
    from .create_report import create_report

    create_report()
