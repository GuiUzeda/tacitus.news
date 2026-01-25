import sys

from loguru import logger


def setup_logger():
    """
    Configures Loguru with a cleaner format and hooks sys.excepthook.
    Call this at the entry point of the application.
    """
    # Remove default handler
    logger.remove()

    # Add a cleaner handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        backtrace=False,  # Set to True for full trace in debug
        diagnose=False,  # Set to True to see variables
    )

    # Hook uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.opt(exception=(exc_type, exc_value, exc_traceback)).critical(
            "Uncaught exception"
        )

    sys.excepthook = handle_exception


# Auto-setup on import if preferred, or explicit call.
# Explicit call is safer for library usage, but for an app, import side-effect is common.
# Let's do explicit call to be clean, or just run it on import since it's a config module.
# Given the user asked for "middleware" style usage, running on import is easiest for the existing scripts.

setup_logger()
