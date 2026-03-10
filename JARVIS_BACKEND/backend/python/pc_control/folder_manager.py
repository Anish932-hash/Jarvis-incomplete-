import os
import shutil
from pathlib import Path
from typing import Dict, Any, List


class FolderManager:

    def create_folder(self, path: str) -> Dict[str, Any]:
        try:
            p = Path(path).resolve()
            p.mkdir(parents=True, exist_ok=True)
            return {"status": "success", "created": str(p)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def delete_folder(self, path: str) -> Dict[str, Any]:
        try:
            p = Path(path).resolve()
            if not p.exists():
                return {"status": "error", "message": "Folder not found"}

            shutil.rmtree(p)
            return {"status": "success", "deleted": str(p)}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def list_folder(self, path: str) -> Dict[str, Any]:
        try:
            p = Path(path).resolve()

            if not p.exists() or not p.is_dir():
                return {"status": "error", "message": "Not a folder"}

            items = [{
                "name": x.name,
                "type": "dir" if x.is_dir() else "file",
                "size": x.stat().st_size
            } for x in p.iterdir()]

            return {"status": "success", "items": items}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def folder_size(self, path: str) -> Dict[str, Any]:
        try:
            p = Path(path).resolve()

            if not p.exists():
                return {"status": "error", "message": "Folder not found"}

            total = 0
            for root, dirs, files in os.walk(p):
                for f in files:
                    total += Path(root, f).stat().st_size

            return {"status": "success", "size_bytes": total}

        except Exception as e:
            return {"status": "error", "message": str(e)}