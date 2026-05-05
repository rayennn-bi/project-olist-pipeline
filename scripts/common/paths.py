import os
from typing import Tuple


def get_data_paths() -> Tuple[str, str]:
    data_path = os.getenv("DATA_PATH")
    if not data_path:
        raise ValueError("DATA_PATH environment variable is not set")

    raw_path = os.path.join(data_path, "raw")
    checkpoint_path = os.path.join(data_path, "checkpoint")
    return raw_path, checkpoint_path