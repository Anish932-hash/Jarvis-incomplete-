import os
import shutil
import hashlib
from pathlib import Path
from datetime import datetime

class FileTools:
    """Advanced safe file utilities: hashing, copying, backups, scanning, merging, splitting."""

    @staticmethod
    def compute_hash(file_path: str, algo: str = "sha256") -> str:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        hash_engine = hashlib.new(algo)
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_engine.update(chunk)
        return hash_engine.hexdigest()

    @staticmethod
    def copy_file(src: str, dst: str, overwrite: bool = False):
        src, dst = Path(src), Path(dst)
        if not src.exists():
            raise FileNotFoundError(f"Source missing: {src}")

        if dst.exists() and not overwrite:
            raise FileExistsError(f"Destination exists: {dst}")

        shutil.copy2(src, dst)
        return True

    @staticmethod
    def backup_file(src: str, backup_dir: str):
        src = Path(src)
        if not src.exists():
            raise FileNotFoundError(src)

        backup_dir = Path(backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{src.stem}_backup_{timestamp}{src.suffix}"

        shutil.copy2(src, backup_path)
        return str(backup_path)

    @staticmethod
    def scan_directory(dir_path: str) -> list:
        path = Path(dir_path)
        if not path.exists():
            raise FileNotFoundError(dir_path)

        return [str(p) for p in path.rglob("*")]

    @staticmethod
    def merge_files(file_list: list, output_path: str):
        with open(output_path, "wb") as out:
            for file in file_list:
                with open(file, "rb") as f:
                    out.write(f.read())
        return output_path

    @staticmethod
    def split_file(file_path: str, chunk_size: int, output_dir: str):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        parts = []
        idx = 1

        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                part_path = Path(output_dir) / f"{Path(file_path).stem}_part{idx}"
                with open(part_path, "wb") as p:
                    p.write(chunk)
                parts.append(str(part_path))
                idx += 1

        return parts