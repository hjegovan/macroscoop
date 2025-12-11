from pathlib import Path
from datetime import datetime, timezone

def project_path(folders: list[str] = []) -> Path:
    """
    Docstring for project_path up too macroscoop
    
    :param folders: Description
    :type folders: list[str]
    :return: Description
    :rtype: Path
    """
    current_dir = Path(__file__)
    project_name = 'macroscoop'
    root_dir = next(p for p in current_dir.parents if p.parts[-1] == project_name)
    
    if folders:
        root_dir = root_dir.joinpath(*folders)
    
    return root_dir 

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

if __name__ == "__main__":
    print(utc_now())