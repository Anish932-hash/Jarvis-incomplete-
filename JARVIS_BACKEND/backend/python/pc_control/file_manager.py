from pathlib import Path
from typing import Any, Dict, List


class FileManager:
    """
    Safe file manager with explicit path checks.
    """

    def read_file(self, path: str, encoding: str = "utf-8") -> Dict[str, Any]:
        try:
            target = Path(path).resolve()
            if not target.exists() or not target.is_file():
                return {"status": "error", "message": "File not found"}
            return {"status": "success", "path": str(target), "content": target.read_text(encoding=encoding)}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def write_file(self, path: str, content: str, encoding: str = "utf-8") -> Dict[str, Any]:
        try:
            target = Path(path).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding=encoding)
            return {"status": "success", "path": str(target), "bytes": len(content.encode(encoding))}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def delete_file(self, path: str) -> Dict[str, Any]:
        try:
            target = Path(path).resolve()
            if not target.exists() or not target.is_file():
                return {"status": "error", "message": "File not found"}
            target.unlink()
            return {"status": "success", "deleted": str(target)}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def list_files(self, path: str) -> Dict[str, Any]:
        try:
            target = Path(path).resolve()
            if not target.exists() or not target.is_dir():
                return {"status": "error", "message": "Directory not found"}
            items: List[str] = [str(p) for p in target.iterdir()]
            return {"status": "success", "items": items}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
