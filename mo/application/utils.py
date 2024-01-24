from pathlib import Path


def dir_not_empty(path: Path) -> bool:
    """Check if the given path is a directory that has files."""
    return path.is_dir() and any(path.iterdir())


def dir_has_more_than_one_file(path: Path) -> bool:
    if not path.is_dir():
        return False
    path_iter = path.iterdir()
    has_one_file = next(path_iter, None) is not None
    return next(path_iter, None) is not None if has_one_file else False


def is_only_file_in_dir(path: Path) -> bool:
    return path.is_file() and dir_has_more_than_one_file(path.parent) is False
