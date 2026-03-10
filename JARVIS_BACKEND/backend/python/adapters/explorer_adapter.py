from __future__ import annotations

import os
import platform
import subprocess
import webbrowser
from pathlib import Path


class ExplorerAdapter:
    """
    Explorer/file-manager adapter for opening folders and revealing files.
    """

    @staticmethod
    def open_path(path: str) -> dict:
        target = Path(path).expanduser()
        if not target.exists():
            return {"status": "error", "message": "path does not exist"}

        system = platform.system().lower()
        if system == "windows":
            os.startfile(str(target))  # type: ignore[attr-defined]
        else:
            webbrowser.open(target.resolve().as_uri(), new=2)

        return {"status": "success", "path": str(target.resolve()), "adapter": "explorer"}

    @staticmethod
    def select_file(path: str) -> dict:
        target = Path(path).expanduser()
        if not target.exists() or not target.is_file():
            return {"status": "error", "message": "path must reference an existing file"}

        system = platform.system().lower()
        if system == "windows":
            subprocess.run(["explorer", f"/select,{str(target.resolve())}"], check=False)
            selected_path = str(target.resolve())
        else:
            # Fallback: open containing directory on non-Windows environments.
            parent = target.resolve().parent
            webbrowser.open(parent.as_uri(), new=2)
            selected_path = str(parent)

        return {"status": "success", "path": selected_path, "adapter": "explorer"}

