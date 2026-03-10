import json
from pathlib import Path
from tempfile import NamedTemporaryFile
import shutil

class FileUtils:

    @staticmethod
    def read_json(path: str):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def write_json(path: str, data: dict):
        tmp = NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(data, tmp, indent=4)
        tmp.close()
        shutil.move(tmp.name, path)
        return True

    @staticmethod
    def calculate_folder_size(path: str):
        p = Path(path)
        total = 0
        for f in p.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
        return total

    @staticmethod
    def clean_directory(path: str):
        p = Path(path)
        for item in p.iterdir():
            if item.is_file():
                item.unlink()
        return True

    @staticmethod
    def merge_json(base: dict, other: dict):
        """Deep merge dicts."""
        for k, v in other.items():
            if isinstance(v, dict) and k in base:
                FileUtils.merge_json(base[k], v)
            else:
                base[k] = v
        return base