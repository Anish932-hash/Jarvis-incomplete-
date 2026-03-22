from __future__ import annotations

import ctypes
import importlib.util
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psutil


class SystemMonitor:
    _POWERSHELL_COMMANDS = ("powershell", "pwsh")

    def cpu_usage(self) -> float:
        return psutil.cpu_percent(interval=0.5)

    def memory_usage(self) -> Dict[str, Any]:
        mem = psutil.virtual_memory()
        return {
            "total": mem.total,
            "available": mem.available,
            "used": mem.used,
            "percent": mem.percent,
        }

    def disk_usage(self) -> Dict[str, Any]:
        usage = {}
        for part in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(part.mountpoint)
                usage[part.device] = {
                    "mountpoint": part.mountpoint,
                    "filesystem": part.fstype,
                    "total": u.total,
                    "used": u.used,
                    "free": u.free,
                    "percent": u.percent,
                }
            except PermissionError:
                continue
        return usage

    def network_usage(self) -> Dict[str, Any]:
        counters = psutil.net_io_counters()
        return {
            "bytes_sent": counters.bytes_sent,
            "bytes_recv": counters.bytes_recv,
            "packets_sent": counters.packets_sent,
            "packets_recv": counters.packets_recv,
        }

    def battery_status(self) -> Dict[str, Any] | None:
        bat = psutil.sensors_battery()
        if not bat:
            return None
        return {
            "percent": bat.percent,
            "plugged_in": bat.power_plugged,
            "secs_left": bat.secsleft,
        }

    def system_info(self) -> Dict[str, Any]:
        return self.machine_profile()

    def machine_profile(self) -> Dict[str, Any]:
        uname = platform.uname()
        cpu_info = self._cpu_info()
        windows_info = self._windows_info()
        gpus = self._gpu_info()
        disks = self._storage_inventory()
        runtimes = self._runtime_inventory()
        permissions = self._permissions_info()
        virtualization = self._virtualization_info(cpu_info=cpu_info)
        dependencies = self._dependency_audit(runtimes=runtimes)
        shell = self._shell_info()
        boot_time = ""
        try:
            boot_time = datetime.fromtimestamp(psutil.boot_time(), tz=timezone.utc).isoformat()
        except Exception:
            boot_time = ""
        memory = self.memory_usage()
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "hostname": socket.gethostname(),
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "platform": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "node": uname.node,
            },
            "windows": windows_info,
            "cpu": cpu_info,
            "memory": {
                "total_bytes": int(memory.get("total", 0) or 0),
                "available_bytes": int(memory.get("available", 0) or 0),
                "used_bytes": int(memory.get("used", 0) or 0),
                "percent": float(memory.get("percent", 0.0) or 0.0),
            },
            "storage": {
                "count": len(disks),
                "items": disks,
                "total_bytes": sum(int(item.get("total_bytes", 0) or 0) for item in disks),
                "free_bytes": sum(int(item.get("free_bytes", 0) or 0) for item in disks),
            },
            "gpu_count": len(gpus),
            "gpus": gpus,
            "runtimes": runtimes,
            "permissions": permissions,
            "virtualization": virtualization,
            "dependencies": dependencies,
            "shell": shell,
            "python": {
                "version": platform.python_version(),
                "executable": sys.executable,
                "prefix": sys.prefix,
                "base_prefix": getattr(sys, "base_prefix", sys.prefix),
            },
            "user": {
                "username": os.environ.get("USERNAME", "") or os.environ.get("USER", ""),
                "home": str(Path.home()),
            },
            "boot_time_utc": boot_time,
            "battery": self.battery_status(),
        }

    def all_metrics(self) -> Dict[str, Any]:
        return {
            "cpu": self.cpu_usage(),
            "memory": self.memory_usage(),
            "disk": self.disk_usage(),
            "network": self.network_usage(),
            "battery": self.battery_status(),
            "system": self.system_info(),
            "timestamp": time.time(),
        }

    def _cpu_info(self) -> Dict[str, Any]:
        fallback = {
            "name": platform.processor(),
            "manufacturer": "",
            "physical_cores": psutil.cpu_count(logical=False) or 0,
            "logical_cores": psutil.cpu_count(logical=True) or 0,
            "max_clock_mhz": 0,
        }
        payload = self._run_powershell_json(
            "$cpu = Get-CimInstance Win32_Processor | "
            "Select-Object Name, Manufacturer, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed, VirtualizationFirmwareEnabled, VMMonitorModeExtensions, SecondLevelAddressTranslationExtensions; "
            "$cpu | ConvertTo-Json -Compress -Depth 4"
        )
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        if not isinstance(payload, dict):
            return fallback
        return {
            "name": str(payload.get("Name", "") or fallback["name"]).strip(),
            "manufacturer": str(payload.get("Manufacturer", "") or "").strip(),
            "physical_cores": int(payload.get("NumberOfCores", fallback["physical_cores"]) or fallback["physical_cores"]),
            "logical_cores": int(payload.get("NumberOfLogicalProcessors", fallback["logical_cores"]) or fallback["logical_cores"]),
            "max_clock_mhz": int(payload.get("MaxClockSpeed", 0) or 0),
            "virtualization_firmware_enabled": bool(payload.get("VirtualizationFirmwareEnabled", False)),
            "vm_monitor_mode_extensions": bool(payload.get("VMMonitorModeExtensions", False)),
            "slat_enabled": bool(payload.get("SecondLevelAddressTranslationExtensions", False)),
        }

    def _windows_info(self) -> Dict[str, Any]:
        fallback = {
            "caption": platform.platform(),
            "version": platform.version(),
            "build_number": "",
            "architecture": platform.machine(),
            "last_boot_utc": "",
        }
        payload = self._run_powershell_json(
            "$os = Get-CimInstance Win32_OperatingSystem | "
            "Select-Object Caption, Version, BuildNumber, OSArchitecture, LastBootUpTime; "
            "$os | ConvertTo-Json -Compress -Depth 4"
        )
        if not isinstance(payload, dict):
            return fallback
        return {
            "caption": str(payload.get("Caption", "") or fallback["caption"]).strip(),
            "version": str(payload.get("Version", "") or fallback["version"]).strip(),
            "build_number": str(payload.get("BuildNumber", "") or "").strip(),
            "architecture": str(payload.get("OSArchitecture", "") or fallback["architecture"]).strip(),
            "last_boot_utc": str(payload.get("LastBootUpTime", "") or fallback["last_boot_utc"]).strip(),
        }

    def _gpu_info(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        try:
            import GPUtil  # type: ignore

            for gpu in GPUtil.getGPUs():
                rows.append(
                    {
                        "name": str(getattr(gpu, "name", "") or "").strip(),
                        "driver": "",
                        "adapter_ram_bytes": int(float(getattr(gpu, "memoryTotal", 0) or 0) * 1024 * 1024),
                        "load": float(getattr(gpu, "load", 0.0) or 0.0),
                        "memory_used_mb": float(getattr(gpu, "memoryUsed", 0.0) or 0.0),
                        "memory_total_mb": float(getattr(gpu, "memoryTotal", 0.0) or 0.0),
                    }
                )
        except Exception:
            rows = []
        if rows:
            return rows
        payload = self._run_powershell_json(
            "$gpu = Get-CimInstance Win32_VideoController | "
            "Select-Object Name, AdapterRAM, DriverVersion, VideoProcessor; "
            "$gpu | ConvertTo-Json -Compress -Depth 4"
        )
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            return []
        normalized: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "name": str(item.get("Name", "") or "").strip(),
                    "driver": str(item.get("DriverVersion", "") or "").strip(),
                    "video_processor": str(item.get("VideoProcessor", "") or "").strip(),
                    "adapter_ram_bytes": int(item.get("AdapterRAM", 0) or 0),
                }
            )
        return normalized

    def _storage_inventory(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for part in psutil.disk_partitions(all=False):
            try:
                usage = psutil.disk_usage(part.mountpoint)
            except PermissionError:
                continue
            rows.append(
                {
                    "device": part.device,
                    "mountpoint": part.mountpoint,
                    "filesystem": part.fstype,
                    "total_bytes": int(usage.total),
                    "used_bytes": int(usage.used),
                    "free_bytes": int(usage.free),
                    "percent": float(usage.percent),
                }
            )
        rows.sort(key=lambda item: str(item.get("mountpoint", "") or "").lower())
        return rows

    def _runtime_inventory(self) -> Dict[str, Any]:
        runtimes = {
            "python": self._command_runtime(["python", "--version"]),
            "py": self._command_runtime(["py", "--version"]),
            "pip": self._command_runtime(["pip", "--version"]),
            "uv": self._command_runtime(["uv", "--version"]),
            "cython": self._command_runtime(["cython", "--version"]),
            "cythonize": self._command_runtime(["cythonize", "--version"]),
            "node": self._command_runtime(["node", "--version"]),
            "npm": self._command_runtime(["npm", "--version"]),
            "pnpm": self._command_runtime(["pnpm", "--version"]),
            "rustc": self._command_runtime(["rustc", "--version"]),
            "cargo": self._command_runtime(["cargo", "--version"]),
            "cl": self._command_runtime(["cl"]),
            "cmake": self._command_runtime(["cmake", "--version"]),
            "ninja": self._command_runtime(["ninja", "--version"]),
            "msbuild": self._command_runtime(["msbuild", "-version"]),
            "git": self._command_runtime(["git", "--version"]),
            "git_lfs": self._command_runtime(["git-lfs", "--version"]),
            "huggingface_cli": self._command_runtime(["huggingface-cli", "--version"]),
            "hf": self._command_runtime(["hf", "--version"]),
            "ollama": self._command_runtime(["ollama", "--version"]),
            "tesseract": self._command_runtime(["tesseract", "--version"]),
            "ffmpeg": self._command_runtime(["ffmpeg", "-version"]),
            "powershell": self._command_runtime(["powershell", "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]),
            "pwsh": self._command_runtime(["pwsh", "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]),
        }
        runtimes["available_count"] = sum(
            1 for value in runtimes.values() if isinstance(value, dict) and bool(value.get("available", False))
        )
        return runtimes

    def _permissions_info(self) -> Dict[str, Any]:
        is_admin = False
        try:
            is_admin = bool(ctypes.windll.shell32.IsUserAnAdmin())  # type: ignore[attr-defined]
        except Exception:
            is_admin = False
        cwd = Path.cwd()
        temp_dir = Path(os.environ.get("TEMP", "")) if os.environ.get("TEMP") else None
        return {
            "is_admin": is_admin,
            "username": os.environ.get("USERNAME", "") or os.environ.get("USER", ""),
            "cwd": str(cwd),
            "cwd_writable": os.access(str(cwd), os.W_OK),
            "temp_dir": str(temp_dir) if temp_dir else "",
            "temp_writable": os.access(str(temp_dir), os.W_OK) if temp_dir else False,
            "venv_active": bool(getattr(sys, "base_prefix", sys.prefix) != sys.prefix),
        }

    def _virtualization_info(self, *, cpu_info: Dict[str, Any]) -> Dict[str, Any]:
        wsl_status = self._command_runtime(["wsl", "--status"])
        return {
            "virtualization_firmware_enabled": bool(cpu_info.get("virtualization_firmware_enabled", False)),
            "vm_monitor_mode_extensions": bool(cpu_info.get("vm_monitor_mode_extensions", False)),
            "slat_enabled": bool(cpu_info.get("slat_enabled", False)),
            "wsl_available": bool(wsl_status.get("available", False)),
            "wsl_status": str(wsl_status.get("version", "") or wsl_status.get("raw", "") or "").strip(),
        }

    def _dependency_audit(self, *, runtimes: Dict[str, Any]) -> Dict[str, Any]:
        python_packages = {
            "cython": importlib.util.find_spec("Cython") is not None,
            "pytesseract": importlib.util.find_spec("pytesseract") is not None,
            "easyocr": importlib.util.find_spec("easyocr") is not None,
            "cv2": importlib.util.find_spec("cv2") is not None,
            "pil": importlib.util.find_spec("PIL") is not None,
        }
        python_ready = bool(asdict := runtimes.get("python", {})) and bool(
            isinstance(asdict, dict) and asdict.get("available", False)
        )
        rust_ready = bool(isinstance(runtimes.get("rustc", {}), dict) and runtimes.get("rustc", {}).get("available", False)) and bool(
            isinstance(runtimes.get("cargo", {}), dict) and runtimes.get("cargo", {}).get("available", False)
        )
        native_build_ready = any(
            bool(isinstance(runtimes.get(name, {}), dict) and runtimes.get(name, {}).get("available", False))
            for name in ("cl", "cmake", "ninja", "msbuild")
        )
        ocr_ready = bool(
            (isinstance(runtimes.get("tesseract", {}), dict) and runtimes.get("tesseract", {}).get("available", False))
            or python_packages["pytesseract"]
            or python_packages["easyocr"]
        )
        vision_ready = bool(python_packages["cv2"] or python_packages["pil"])
        missing = [
            name
            for name, ready in {
                "python_runtime": python_ready,
                "rust_toolchain": rust_ready,
                "native_build_toolchain": native_build_ready,
                "ocr_runtime": ocr_ready,
                "vision_runtime": vision_ready,
                "cython": bool(
                    python_packages["cython"]
                    or (isinstance(runtimes.get("cython", {}), dict) and runtimes.get("cython", {}).get("available", False))
                ),
            }.items()
            if not ready
        ]
        return {
            "python_packages": python_packages,
            "python_ready": python_ready,
            "rust_ready": rust_ready,
            "native_build_ready": native_build_ready,
            "ocr_ready": ocr_ready,
            "vision_ready": vision_ready,
            "missing": missing,
            "ready_count": 6 - len(missing),
            "total_checks": 6,
        }

    def _shell_info(self) -> Dict[str, Any]:
        powershell_runtime = self._command_runtime(
            ["powershell", "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]
        )
        pwsh_runtime = self._command_runtime(
            ["pwsh", "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"]
        )
        return {
            "comspec": os.environ.get("ComSpec", ""),
            "current_shell": os.environ.get("SHELL", "") or os.environ.get("ComSpec", ""),
            "terminal_program": os.environ.get("TERM_PROGRAM", "") or os.environ.get("WT_SESSION", ""),
            "powershell_available": bool(powershell_runtime.get("available", False)),
            "powershell_version": str(powershell_runtime.get("version", "") or "").strip(),
            "pwsh_available": bool(pwsh_runtime.get("available", False)),
            "pwsh_version": str(pwsh_runtime.get("version", "") or "").strip(),
        }

    def _command_runtime(self, command: List[str]) -> Dict[str, Any]:
        executable = str(command[0] or "").strip() if command else ""
        resolved = str(shutil.which(executable) or "").strip() if executable else ""
        payload = {
            "command": " ".join(command),
            "executable": executable,
            "path": resolved,
            "available": bool(resolved),
            "version": "",
            "raw": "",
        }
        if not resolved:
            return payload
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=8,
                check=False,
            )
        except Exception:
            return payload
        output = str(completed.stdout or completed.stderr or "").strip()
        payload["raw"] = output
        payload["version"] = output.splitlines()[0].strip() if output else ""
        payload["available"] = completed.returncode == 0 or bool(payload["version"])
        return payload

    def _run_powershell_json(self, script: str) -> Any:
        clean_script = str(script or "").strip()
        if not clean_script:
            return {}
        for executable in self._POWERSHELL_COMMANDS:
            resolved = shutil.which(executable)
            if not resolved:
                continue
            try:
                completed = subprocess.run(
                    [resolved, "-NoProfile", "-Command", clean_script],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=12,
                    check=False,
                )
            except Exception:
                continue
            stdout = str(completed.stdout or "").strip()
            if not stdout:
                continue
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                continue
        return {}
