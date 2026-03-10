import os
import subprocess
import winreg
import shutil
from pathlib import Path
from typing import Optional, Dict, Any


class AppLauncher:
    """Advanced Windows application launcher with registry + path resolution."""

    COMMON_PATHS = [
        r"C:\Program Files",
        r"C:\Program Files (x86)",
        r"C:\Windows\System32",
        r"C:\Windows",
    ]

    def launch(self, app_name: str) -> Dict[str, Any]:
        """Launch application by name or absolute path."""
        try:
            if os.path.isfile(app_name):
                subprocess.Popen([app_name], shell=True)
                return {"status": "success", "path": app_name}

            resolved = self.resolve_app_path(app_name)
            if resolved:
                subprocess.Popen([resolved], shell=True)
                return {"status": "success", "path": resolved}

            uwp = self.launch_uwp(app_name)
            if uwp:
                return uwp

            return {"status": "error", "message": "Application not found"}

        except Exception as e:
            return {"status": "error", "message": str(e)}

    def resolve_app_path(self, name: str) -> Optional[str]:
        """Search system PATH and Program Files."""
        exe_name = name if name.endswith(".exe") else f"{name}.exe"

        # Check PATH
        found = shutil.which(exe_name)
        if found:
            return found

        # Search common directories
        for base in self.COMMON_PATHS:
            for root, dirs, files in os.walk(base):
                if exe_name in files:
                    return os.path.join(root, exe_name)

        return None

    def launch_uwp(self, app_name: str) -> Optional[Dict[str, Any]]:
        """Try launching UWP apps via shell."""
        commands = [
            f"shell:AppsFolder\\{app_name}!App",
            f"shell:AppsFolder\\Microsoft.{app_name}!App"
        ]

        for cmd in commands:
            try:
                subprocess.Popen(["explorer.exe", cmd])
                return {"status": "success", "uwp": cmd}
            except Exception:
                continue

        return None