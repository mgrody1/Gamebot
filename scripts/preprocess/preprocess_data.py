# ruff: noqa: E402

import logging
import sys
from pathlib import Path

# Add the base directory to sys.path
base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.append(str(base_dir))

# Repo imports
from gamebot_core.log_utils import setup_logging  # noqa: E402
from preprocess_data_helper import get_castaway_features  # noqa: E402

setup_logging(logging.DEBUG)  # Use the desired logging level
logger = logging.getLogger(__name__)


# Main script
if __name__ == "__main__":
    # Establish a database connection
    logger.info("Establishing db connection")

    try:
        castaway_features = get_castaway_features()
        logger.info("castaway_features read-in")
    except Exception as exc:
        logger.error("An error occurred: %s", exc)
