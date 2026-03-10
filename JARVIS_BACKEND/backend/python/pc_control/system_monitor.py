import psutil
import platform
import time
from typing import Dict, Any


class SystemMonitor:

    def cpu_usage(self) -> float:
        return psutil.cpu_percent(interval=0.5)

    def memory_usage(self) -> Dict[str, Any]:
        mem = psutil.virtual_memory()
        return {
            "total": mem.total,
            "used": mem.used,
            "percent": mem.percent
        }

    def disk_usage(self) -> Dict[str, Any]:
        usage = {}
        for part in psutil.disk_partitions(all=False):
            try:
                u = psutil.disk_usage(part.mountpoint)
                usage[part.device] = {
                    "total": u.total,
                    "used": u.used,
                    "percent": u.percent
                }
            except PermissionError:
                continue
        return usage

    def network_usage(self) -> Dict[str, Any]:
        counters = psutil.net_io_counters()
        return {
            "bytes_sent": counters.bytes_sent,
            "bytes_recv": counters.bytes_recv
        }

    def battery_status(self) -> Dict[str, Any] | None:
        bat = psutil.sensors_battery()
        if not bat:
            return None
        return {
            "percent": bat.percent,
            "plugged_in": bat.power_plugged,
            "secs_left": bat.secsleft
        }

    def system_info(self) -> Dict[str, Any]:
        return {
            "os": platform.platform(),
            "cpu": platform.processor(),
            "machine": platform.machine(),
            "python": platform.python_version()
        }

    def all_metrics(self) -> Dict[str, Any]:
        return {
            "cpu": self.cpu_usage(),
            "memory": self.memory_usage(),
            "disk": self.disk_usage(),
            "network": self.network_usage(),
            "battery": self.battery_status(),
            "system": self.system_info(),
            "timestamp": time.time()
        }