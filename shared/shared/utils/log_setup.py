import logging
import logging.config
import json
from pathlib import Path

from shared.utils.helper import project_path


def setup_logging(name: str = "", config_name: str = "log_config.json") -> logging.Logger:
    """
    Setup logging configuration from JSON file.
    
    Args:
        config_name: Name of the logging config file
    """
    if name == "":
        name = __name__
    
    log_dir = project_path(["shared","shared","data","logs"])
    print(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    config_path = project_path(["shared","shared","config",config_name])
    
    if not config_path.exists():
        raise FileNotFoundError(f"Logging config not found: {config_path}")
    
    with open(config_path, "r") as f:
        config = json.load(f)
        x = config['handlers']['file']['filename']
        config['handlers']['file']['filename'] = project_path(["shared","shared",x])
    
    # Apply configuration
    logging.config.dictConfig(config)
    
    return logging.getLogger(name)