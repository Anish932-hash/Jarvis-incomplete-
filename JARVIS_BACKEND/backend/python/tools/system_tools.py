import psutil
import platform
import asyncio
import time
from datetime import datetime

class SystemTools:
    """Safe system information + monitoring + resource checks."""

    @staticmethod
    def get_system_info():
        return {
            "os": platform.platform(),
            "cpu": platform.processor(),
            "cores": psutil.cpu_count(logical=False),
            "threads": psutil.cpu_count(),
            "memory_total": psutil.virtual_memory().total,
            "gpu": SystemTools.get_gpu_info()
        }

    @staticmethod
    def get_gpu_info():
        try:
            import GPUtil
            gpus = GPUtil.getGPUs()
            if not gpus:
                return None
            return [{
                "name": g.name,
                "load": g.load,
                "memory_total": g.memoryTotal,
                "memory_used": g.memoryUsed
            } for g in gpus]
        except Exception:
            return None

    @staticmethod
    def get_resource_usage():
        mem = psutil.virtual_memory()
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.3),
            "memory_percent": mem.percent,
            "memory_free": mem.available,
            "disk_usage": psutil.disk_usage("/")._asdict(),
            "process_count": len(psutil.pids())
        }

    @staticmethod
    async def monitor_resources(callback, interval=2):
        """Async real-time monitoring."""
        while True:
            usage = SystemTools.get_resource_usage()
            callback(usage)
            await asyncio.sleep(interval)

    @staticmethod
    def list_processes(limit=40):
        procs = []
        for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
            procs.append(proc.info)
        return sorted(procs, key=lambda p: p["cpu_percent"], reverse=True)[:limit]