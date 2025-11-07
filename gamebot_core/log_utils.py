import logging
import os
import sys
from pathlib import Path


def setup_logging(log_level=logging.INFO, log_filename="pipeline.log"):
    """Configure root logging for Gamebot.

    Output is emitted to stdout and to a file inside ``run_logs/`` (or
    ``$GAMEBOT_RUN_LOG_DIR`` when set) so Docker, Dev Containers, and local
    workflows share the same artefacts.
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger()
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    # Always set up stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Try to set up file handler, but gracefully handle permission errors
    try:
        log_dir = get_run_log_dir()
        log_path = log_dir / log_filename
        file_handler = logging.FileHandler(log_path, mode="w")  # Overwrite on each run
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except PermissionError as e:
        # In container environments or restricted permissions, just log to stdout
        logger.warning(
            f"Could not create log file {log_filename}: {e}. Logging to stdout only."
        )

    # Suppress overly verbose logs from third-party libraries
    for lib in ["urllib3", "botocore", "s3transfer"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Hook into uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Let Ctrl+C exit cleanly
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
        )
        # Flush all handlers to ensure logs are written
        for handler in logger.handlers:
            handler.flush()
            handler.close()

    sys.excepthook = handle_exception


_RUN_LOG_DIR = Path(os.getenv("GAMEBOT_RUN_LOG_DIR", "run_logs"))


def get_run_log_dir() -> Path:
    """
    Return the root directory for pipeline run artefacts (logs, validation reports).
    The location defaults to `run_logs/` at the repo root but can be overridden with
    the GAMEBOT_RUN_LOG_DIR environment variable.
    """
    # Local development or container deployments both use the configured path
    try:
        _RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
        return _RUN_LOG_DIR
    except PermissionError:
        # If we can't create the directory due to permissions, fall back to temp directory
        import tempfile

        temp_dir = Path(tempfile.gettempdir()) / "gamebot_logs"
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir
