import logging
import logging.config
from .settings import settings  # Relative import; assumes same package


def setup_logging():
    """
    Setup logging configuration based on settings.
    Logs to both console and file for all levels >= log_level.
    Uses dictConfig for flexibility; can be extended for rotation if needed.
    """
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,  # Preserve root logger if needed
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": settings.log_level,
            },
            "file": {
                "class": "logging.FileHandler",
                "formatter": "default",
                "filename": settings.log_file,
                "level": settings.log_level,
                "mode": "a",  # Append to log file across runs
            },
        },
        "root": {"level": settings.log_level, "handlers": ["console", "file"]},
    }
    logging.config.dictConfig(log_config)


# Initialize logging immediately
setup_logging()

# Global logger instance for the module
logger = logging.getLogger(__name__)
# Comment: Use logger.info/error in all modules; e.g., logger.error("Request failed: %s", exc_info=True)
