import logging
import logging.config
import json
from pathlib import Path


def setup_logging(name: str = "", config_name: str = "log_config.json") -> logging.Logger:
    """
    Setup logging configuration from JSON file.
    
    Args:
        config_name: Name of the logging config file
    """
    if name == "":
        name = __name__
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    
    log_dir = project_root / "shared" / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    config_path = project_root / "shared" / "config" / config_name
    
    if not config_path.exists():
        raise FileNotFoundError(f"Logging config not found: {config_path}")
    
    with open(config_path, "r") as f:
        config = json.load(f)
        x = config['handlers']['file']['filename']
        config['handlers']['file']['filename'] = project_root / "shared" / x
    
    # Apply configuration
    logging.config.dictConfig(config)
    
    return logging.getLogger(name)