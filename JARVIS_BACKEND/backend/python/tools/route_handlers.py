import os
import re
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psutil

from backend.python.adapters import BrowserAdapter, ExplorerAdapter
from backend.python.core.tool_registry import ToolRegistry
from backend.python.router import route


_MODEL_ROUTER_CACHE_LOCK = threading.RLock()
_MODEL_ROUTER_CACHE: Dict[str, Any] = {
    "created_at": 0.0,
    "manager": None,
    "registry": None,
    "router": None,
}


def _path_roots() -> List[Path]:
    roots: List[Path] = [Path.home().resolve(), Path.cwd().resolve()]
    workspace = os.getenv("JARVIS_WORKSPACE_ROOT")
    if workspace:
        try:
            roots.append(Path(workspace).expanduser().resolve())
        except Exception:
            pass
    return roots


def _resolve_safe_path(raw_path: str) -> Tuple[bool, str | Path]:
    if not raw_path:
        return (False, "Path is required.")
    try:
        resolved = Path(raw_path).expanduser().resolve()
    except Exception as exc:  # noqa: BLE001
        return (False, f"Invalid path: {exc}")

    if os.getenv("JARVIS_ALLOW_ANY_PATH") == "1":
        return (True, resolved)

    for root in _path_roots():
        if resolved == root or root in resolved.parents:
            return (True, resolved)

    roots_msg = ", ".join(str(root) for root in _path_roots())
    return (False, f"Path is outside allowed roots. Allowed roots: {roots_msg}")


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_url(raw_url: str) -> str:
    value = raw_url.strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value
    return f"https://{value}"


def _import_pyautogui():
    try:
        import pyautogui  # type: ignore

        pyautogui.FAILSAFE = True
        pyautogui.PAUSE = 0.05
        return pyautogui
    except Exception as exc:  # noqa: BLE001
        return exc


def _open_app(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.app_launcher import AppLauncher

    app_name = payload.get("app_name", "notepad")
    return AppLauncher().launch(str(app_name))


def _open_url(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw_url = str(payload.get("url", "")).strip()
    url = _normalize_url(raw_url)
    if not url:
        return {"status": "error", "message": "url is required"}
    return BrowserAdapter.open_url(url, new_tab=True)


def _media_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    query = str(payload.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "Query is required."}
    url = f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"
    webbrowser.open(url, new=2)
    return {"status": "success", "url": url, "query": query}


def _defender_status(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.defender_monitor import DefenderMonitor

    return DefenderMonitor().get_status()


def _system_snapshot(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.system_monitor import SystemMonitor

    metrics = SystemMonitor().all_metrics()
    return {"status": "success", "metrics": metrics}


def _list_processes(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.system_tools import SystemTools

    limit = _to_int(payload.get("limit", 20), 20)
    limit = max(1, min(limit, 200))
    processes = SystemTools.list_processes(limit=limit)
    process_names = [str(item.get("name", "")).lower() for item in processes if isinstance(item, dict)]
    return {
        "status": "success",
        "limit": limit,
        "processes": processes,
        "process_names": process_names,
        "count": len(processes),
    }


def _terminate_process(payload: Dict[str, Any]) -> Dict[str, Any]:
    pid_value = payload.get("pid")
    name_value = str(payload.get("name", "")).strip().lower()
    max_count = _to_int(payload.get("max_count", 3), 3)
    max_count = max(1, min(max_count, 20))
    current_pid = os.getpid()

    terminated: List[Dict[str, Any]] = []
    errors: List[str] = []

    def _terminate(proc: psutil.Process) -> None:
        try:
            if proc.pid == current_pid:
                return
            info_name = proc.name()
            proc.terminate()
            try:
                proc.wait(timeout=2.5)
            except psutil.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2.0)
            terminated.append({"pid": proc.pid, "name": info_name})
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{proc.pid}: {exc}")

    if pid_value is not None:
        pid = _to_int(pid_value, -1)
        if pid <= 0:
            return {"status": "error", "message": "pid must be a positive integer"}
        try:
            _terminate(psutil.Process(pid))
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
    elif name_value:
        for proc in psutil.process_iter(["pid", "name"]):
            try:
                proc_name = str(proc.info.get("name", "")).strip().lower()
            except Exception:  # noqa: BLE001
                continue
            if not proc_name:
                continue
            if name_value not in proc_name:
                continue
            _terminate(proc)
            if len(terminated) >= max_count:
                break
    else:
        return {"status": "error", "message": "Either pid or name is required."}

    if not terminated:
        if errors:
            return {"status": "error", "message": "; ".join(errors[:5])}
        return {"status": "error", "message": "No matching process terminated"}

    return {"status": "success", "terminated": terminated, "count": len(terminated), "errors": errors[:5]}


def _list_windows(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    limit = _to_int(payload.get("limit", 60), 60)
    limit = max(1, min(limit, 300))
    windows = WindowManager().list_windows()[:limit]
    return {"status": "success", "count": len(windows), "windows": windows}


def _active_window(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    info = WindowManager().active_window()
    if isinstance(info, dict) and info.get("status") == "error":
        return info
    return {"status": "success", "window": info}


def _window_topology(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    limit = _to_int(payload.get("limit", 80), 80)
    include_windows = _to_bool(payload.get("include_windows", False))
    return WindowManager().window_topology_snapshot(
        query=str(payload.get("query", "")).strip(),
        app_name=str(payload.get("app_name", "")).strip(),
        window_title=str(payload.get("window_title", "")).strip(),
        include_windows=include_windows,
        limit=limit,
    )


def _reacquire_window(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    limit = _to_int(payload.get("limit", 80), 80)
    include_candidates = _to_bool(payload.get("include_candidates", True), True)
    benchmark_guidance = (
        dict(payload.get("benchmark_guidance", {}))
        if isinstance(payload.get("benchmark_guidance", {}), dict)
        else {}
    )
    hwnd_raw = payload.get("hwnd")
    pid_raw = payload.get("pid")
    parent_hwnd_raw = payload.get("parent_hwnd")
    parent_pid_raw = payload.get("parent_pid")
    return WindowManager().reacquire_window(
        app_name=str(payload.get("app_name", "")).strip(),
        window_title=str(payload.get("window_title", "")).strip(),
        query=str(payload.get("query", "")).strip(),
        window_signature=str(payload.get("window_signature", "")).strip(),
        hwnd=None if hwnd_raw in {None, ""} else _to_int(hwnd_raw, 0),
        pid=None if pid_raw in {None, ""} else _to_int(pid_raw, 0),
        parent_hwnd=None if parent_hwnd_raw in {None, ""} else _to_int(parent_hwnd_raw, 0),
        parent_pid=None if parent_pid_raw in {None, ""} else _to_int(parent_pid_raw, 0),
        benchmark_guidance=benchmark_guidance,
        include_candidates=include_candidates,
        limit=limit,
    )


def _focus_window(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    title = str(payload.get("title", "")).strip()
    hwnd_raw = payload.get("hwnd")
    hwnd = None if hwnd_raw is None else _to_int(hwnd_raw, -1)
    if hwnd is not None and hwnd <= 0:
        return {"status": "error", "message": "hwnd must be a positive integer"}
    return WindowManager().focus_window(title_contains=title, hwnd=hwnd)


def _focus_related_window_impl(payload: Dict[str, Any], *, force_chain: bool = False) -> Dict[str, Any]:
    from backend.python.pc_control.window_manager import WindowManager

    hwnd_raw = payload.get("hwnd")
    pid_raw = payload.get("pid")
    hwnd = None if hwnd_raw in {None, ""} else _to_int(hwnd_raw, -1)
    pid = None if pid_raw in {None, ""} else _to_int(pid_raw, -1)
    if hwnd is not None and hwnd <= 0:
        return {"status": "error", "message": "hwnd must be a positive integer"}
    if pid is not None and pid <= 0:
        return {"status": "error", "message": "pid must be a positive integer"}
    benchmark_guidance = (
        dict(payload.get("benchmark_guidance", {}))
        if isinstance(payload.get("benchmark_guidance", {}), dict)
        else {}
    )
    preferred_title = str(
        payload.get("preferred_title", "")
        or payload.get("title", "")
        or ""
    ).strip()
    requested_chain = _to_bool(payload.get("follow_descendant_chain", False), default=False)
    requested_max_chain_steps = max(
        1,
        min(_to_int(payload.get("max_descendant_focus_steps", 1), 1), 6),
    )
    return WindowManager().focus_related_window(
        query=str(payload.get("query", "")).strip(),
        app_name=str(payload.get("app_name", "")).strip(),
        window_title=str(payload.get("window_title", "")).strip(),
        title_contains=str(payload.get("title", "")).strip(),
        hint_query=str(payload.get("hint_query", "")).strip(),
        descendant_hint_query=str(payload.get("descendant_hint_query", "")).strip(),
        campaign_hint_query=str(payload.get("campaign_hint_query", "")).strip(),
        campaign_preferred_title=str(payload.get("campaign_preferred_title", "")).strip(),
        preferred_title=preferred_title,
        hwnd=hwnd,
        pid=pid,
        follow_descendant_chain=force_chain or requested_chain,
        max_descendant_focus_steps=(
            max(2, requested_max_chain_steps)
            if force_chain
            else requested_max_chain_steps
        ),
        benchmark_guidance=benchmark_guidance,
        limit=max(1, min(_to_int(payload.get("limit", 80), 80), 240)),
    )


def _focus_related_window(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _focus_related_window_impl(payload, force_chain=False)


def _focus_related_window_chain(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _focus_related_window_impl(payload, force_chain=True)


async def _media_info(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().get_media_info()


async def _media_play_pause(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().play_pause()


async def _media_play(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().play()


async def _media_pause(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().pause()


async def _media_stop(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().stop()


async def _media_next(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().next_track()


async def _media_previous(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.media_control import MediaController

    return await MediaController().previous_track()


def _send_notification(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.notification_manager import NotificationManager

    title = str(payload.get("title", "JARVIS")).strip() or "JARVIS"
    message = str(payload.get("message", "")).strip()
    if not message:
        return {"status": "error", "message": "message is required"}
    icon_path = payload.get("icon_path")
    return NotificationManager().send_notification(title=title, message=message, icon_path=icon_path)


def _search_files(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.search_tools import SearchTools

    base_dir = str(payload.get("base_dir", str(Path.home())))
    pattern = str(payload.get("pattern", "*")).strip() or "*"
    max_results = _to_int(payload.get("max_results", 250), 250)
    max_results = max(1, min(max_results, 5000))

    ok, safe_path = _resolve_safe_path(base_dir)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    try:
        matches = SearchTools.search_files(str(safe_path), pattern)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {
        "status": "success",
        "base_dir": str(safe_path),
        "pattern": pattern,
        "max_results": max_results,
        "count": len(matches),
        "results": matches[:max_results],
        "truncated": len(matches) > max_results,
    }


def _search_text(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.search_tools import SearchTools

    base_dir = str(payload.get("base_dir", str(Path.home())))
    keyword = str(payload.get("keyword", "")).strip()
    if not keyword:
        return {"status": "error", "message": "keyword is required"}
    extensions = payload.get("extensions")
    max_results = _to_int(payload.get("max_results", 300), 300)
    max_results = max(1, min(max_results, 5000))

    ok, safe_path = _resolve_safe_path(base_dir)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    try:
        matches = SearchTools.search_text_in_files(str(safe_path), keyword, extensions=extensions)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {
        "status": "success",
        "base_dir": str(safe_path),
        "keyword": keyword,
        "max_results": max_results,
        "count": len(matches),
        "results": matches[:max_results],
        "truncated": len(matches) > max_results,
    }


def _scan_directory(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.file_tools import FileTools

    target = str(payload.get("path", str(Path.home()))).strip()
    max_results = _to_int(payload.get("max_results", 1000), 1000)
    max_results = max(1, min(max_results, 10000))

    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    try:
        files = FileTools.scan_directory(str(safe_path))
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {
        "status": "success",
        "path": str(safe_path),
        "max_results": max_results,
        "count": len(files),
        "results": files[:max_results],
        "truncated": len(files) > max_results,
    }


def _hash_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.file_tools import FileTools

    target = str(payload.get("path", "")).strip()
    algo = str(payload.get("algo", "sha256")).strip() or "sha256"

    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    if not Path(safe_path).is_file():
        return {"status": "error", "message": "path must reference a file"}

    try:
        digest = FileTools.compute_hash(str(safe_path), algo=algo)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {"status": "success", "path": str(safe_path), "algo": algo, "hash": digest}


def _backup_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.file_tools import FileTools

    src = str(payload.get("source", "")).strip()
    backup_dir = str(payload.get("backup_dir", str(Path.home() / "jarvis_backups"))).strip()

    ok_src, safe_src = _resolve_safe_path(src)
    if not ok_src:
        return {"status": "error", "message": str(safe_src)}
    ok_dst, safe_dst = _resolve_safe_path(backup_dir)
    if not ok_dst:
        return {"status": "error", "message": str(safe_dst)}

    try:
        backup_path = FileTools.backup_file(str(safe_src), str(safe_dst))
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {"status": "success", "source": str(safe_src), "backup_path": backup_path}


def _copy_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.file_tools import FileTools

    src = str(payload.get("source", "")).strip()
    dst = str(payload.get("destination", "")).strip()
    overwrite = bool(payload.get("overwrite", False))
    if not src or not dst:
        return {"status": "error", "message": "source and destination are required"}

    ok_src, safe_src = _resolve_safe_path(src)
    if not ok_src:
        return {"status": "error", "message": str(safe_src)}
    ok_dst, safe_dst = _resolve_safe_path(dst)
    if not ok_dst:
        return {"status": "error", "message": str(safe_dst)}
    if not Path(safe_src).is_file():
        return {"status": "error", "message": "source must be a file"}

    try:
        FileTools.copy_file(str(safe_src), str(safe_dst), overwrite=overwrite)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {"status": "success", "source": str(safe_src), "destination": str(safe_dst), "overwrite": overwrite}


def _list_folder(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.folder_manager import FolderManager

    target = str(payload.get("path", str(Path.home()))).strip()
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    result = FolderManager().list_folder(str(safe_path))
    if result.get("status") == "success":
        items = result.get("items")
        result["path"] = str(safe_path)
        result["count"] = len(items) if isinstance(items, list) else 0
    return result


def _create_folder(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.folder_manager import FolderManager

    target = str(payload.get("path", "")).strip()
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    return FolderManager().create_folder(str(safe_path))


def _folder_size(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.folder_manager import FolderManager

    target = str(payload.get("path", str(Path.home()))).strip()
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    result = FolderManager().folder_size(str(safe_path))
    if result.get("status") == "success":
        result["path"] = str(safe_path)
    return result


def _explorer_open_path(payload: Dict[str, Any]) -> Dict[str, Any]:
    target = str(payload.get("path", str(Path.home()))).strip()
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    return ExplorerAdapter.open_path(str(safe_path))


def _explorer_select_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    target = str(payload.get("path", "")).strip()
    if not target:
        return {"status": "error", "message": "path is required"}
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    return ExplorerAdapter.select_file(str(safe_path))


def _list_files(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.file_manager import FileManager

    target = str(payload.get("path", str(Path.home()))).strip()
    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}
    result = FileManager().list_files(str(safe_path))
    if result.get("status") == "success":
        items = result.get("items")
        result["path"] = str(safe_path)
        result["count"] = len(items) if isinstance(items, list) else 0
    return result


def _read_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.file_manager import FileManager

    target = str(payload.get("path", "")).strip()
    encoding = str(payload.get("encoding", "utf-8")).strip() or "utf-8"
    max_chars = _to_int(payload.get("max_chars", 120_000), 120_000)
    max_chars = max(1024, min(max_chars, 500_000))

    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    result = FileManager().read_file(str(safe_path), encoding=encoding)
    if result.get("status") != "success":
        return result

    raw_content = str(result.get("content", ""))
    truncated = len(raw_content) > max_chars
    content = raw_content[:max_chars] if truncated else raw_content
    return {
        "status": "success",
        "path": str(safe_path),
        "encoding": encoding,
        "content": content,
        "chars": len(content),
        "truncated": truncated,
    }


def _write_file(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.pc_control.file_manager import FileManager

    target = str(payload.get("path", "")).strip()
    content_raw = payload.get("content")
    if content_raw is None:
        return {"status": "error", "message": "content is required"}
    content = str(content_raw)
    encoding = str(payload.get("encoding", "utf-8")).strip() or "utf-8"
    overwrite = bool(payload.get("overwrite", True))
    max_bytes = _to_int(payload.get("max_bytes", 2_000_000), 2_000_000)
    max_bytes = max(1024, min(max_bytes, 10_000_000))

    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    try:
        content_bytes = len(content.encode(encoding))
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    if content_bytes > max_bytes:
        return {"status": "error", "message": f"content exceeds max_bytes={max_bytes}"}

    if not overwrite and Path(safe_path).exists():
        return {"status": "error", "message": "target file exists and overwrite=false"}

    result = FileManager().write_file(str(safe_path), content, encoding=encoding)
    if result.get("status") != "success":
        return result
    result["overwrite"] = overwrite
    return result


def _time_now(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.time_tools import TimeTools

    tz = str(payload.get("timezone", "UTC")).strip() or "UTC"
    try:
        now_dt = TimeTools.now(tz)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    return {"status": "success", "timezone": tz, "iso": now_dt.isoformat()}


def _clipboard_read(_: Dict[str, Any]) -> Dict[str, Any]:
    try:
        import pyperclip  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": f"pyperclip unavailable: {exc}"}

    try:
        value = pyperclip.paste()
        text = "" if value is None else str(value)
        return {"status": "success", "text": text, "chars": len(text)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _clipboard_write(payload: Dict[str, Any]) -> Dict[str, Any]:
    text = payload.get("text")
    if text is None:
        return {"status": "error", "message": "text is required"}

    try:
        import pyperclip  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": f"pyperclip unavailable: {exc}"}

    try:
        value = str(text)
        pyperclip.copy(value)
        return {"status": "success", "chars": len(value)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
    text = str(payload.get("text", ""))
    if not text:
        return {"status": "error", "message": "text is required"}

    pyautogui_mod = _import_pyautogui()
    if isinstance(pyautogui_mod, Exception):
        return {"status": "error", "message": f"pyautogui unavailable: {pyautogui_mod}"}

    interval_raw = payload.get("interval", 0.02)
    try:
        interval = max(0.0, min(float(interval_raw), 1.0))
    except Exception:  # noqa: BLE001
        interval = 0.02
    press_enter = bool(payload.get("press_enter", False))

    try:
        pyautogui_mod.write(text, interval=interval)
        if press_enter:
            pyautogui_mod.press("enter")
        return {"status": "success", "chars": len(text), "press_enter": press_enter}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
    keys_raw = payload.get("keys")
    if isinstance(keys_raw, str):
        keys = [part.strip().lower() for part in re.split(r"[+,]", keys_raw) if part.strip()]
    elif isinstance(keys_raw, list):
        keys = [str(part).strip().lower() for part in keys_raw if str(part).strip()]
    else:
        key = str(payload.get("key", "")).strip().lower()
        keys = [key] if key else []

    if not keys:
        return {"status": "error", "message": "keys or key is required"}

    pyautogui_mod = _import_pyautogui()
    if isinstance(pyautogui_mod, Exception):
        return {"status": "error", "message": f"pyautogui unavailable: {pyautogui_mod}"}

    try:
        if len(keys) == 1:
            pyautogui_mod.press(keys[0])
        else:
            pyautogui_mod.hotkey(*keys)
        return {"status": "success", "keys": keys}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _mouse_move(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "x" not in payload or "y" not in payload:
        return {"status": "error", "message": "x and y are required"}

    try:
        x = int(payload.get("x"))
        y = int(payload.get("y"))
    except Exception:  # noqa: BLE001
        return {"status": "error", "message": "x and y must be integers"}

    try:
        duration = max(0.0, min(float(payload.get("duration", 0.05)), 5.0))
    except Exception:  # noqa: BLE001
        duration = 0.05

    pyautogui_mod = _import_pyautogui()
    if isinstance(pyautogui_mod, Exception):
        return {"status": "error", "message": f"pyautogui unavailable: {pyautogui_mod}"}

    try:
        pyautogui_mod.moveTo(x, y, duration=duration)
        return {"status": "success", "x": x, "y": y, "duration": duration}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _mouse_click(payload: Dict[str, Any]) -> Dict[str, Any]:
    pyautogui_mod = _import_pyautogui()
    if isinstance(pyautogui_mod, Exception):
        return {"status": "error", "message": f"pyautogui unavailable: {pyautogui_mod}"}

    button = str(payload.get("button", "left")).strip().lower() or "left"
    if button not in {"left", "right", "middle"}:
        return {"status": "error", "message": "button must be left, right, or middle"}

    try:
        clicks = max(1, min(int(payload.get("clicks", 1)), 5))
    except Exception:  # noqa: BLE001
        clicks = 1
    try:
        interval = max(0.0, min(float(payload.get("interval", 0.05)), 1.0))
    except Exception:  # noqa: BLE001
        interval = 0.05

    x_value = payload.get("x")
    y_value = payload.get("y")
    x = None
    y = None
    if x_value is not None and y_value is not None:
        try:
            x = int(x_value)
            y = int(y_value)
        except Exception:  # noqa: BLE001
            return {"status": "error", "message": "x and y must be integers when provided"}

    try:
        pyautogui_mod.click(x=x, y=y, clicks=clicks, interval=interval, button=button)
        return {"status": "success", "x": x, "y": y, "clicks": clicks, "button": button}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _mouse_scroll(payload: Dict[str, Any]) -> Dict[str, Any]:
    amount_raw = payload.get("amount", 500)
    try:
        amount = int(amount_raw)
    except Exception:  # noqa: BLE001
        return {"status": "error", "message": "amount must be an integer"}

    pyautogui_mod = _import_pyautogui()
    if isinstance(pyautogui_mod, Exception):
        return {"status": "error", "message": f"pyautogui unavailable: {pyautogui_mod}"}

    try:
        pyautogui_mod.scroll(amount)
        return {"status": "success", "amount": amount}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _screenshot_capture(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.vision_tools import VisionTools

    raw_path = str(payload.get("path", "")).strip()
    if not raw_path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = str(Path.home() / "Pictures" / f"jarvis_capture_{timestamp}.png")

    ok, safe_path = _resolve_safe_path(raw_path)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    region_raw = payload.get("region")
    region = None
    if isinstance(region_raw, (list, tuple)) and len(region_raw) == 4:
        try:
            x = int(region_raw[0])
            y = int(region_raw[1])
            width = int(region_raw[2])
            height = int(region_raw[3])
            if width > 0 and height > 0:
                region = (x, y, x + width, y + height)
        except Exception:
            return {"status": "error", "message": "region must be [x, y, width, height] integers"}

    try:
        path = VisionTools.save_screenshot(str(safe_path), region=region)
        return {"status": "success", "path": path, "region": region_raw if region is not None else None}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _browser_read_dom(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_tools import BrowserTools

    url = str(payload.get("url", "")).strip()
    if not url:
        return {"status": "error", "message": "url is required"}
    max_chars = _to_int(payload.get("max_chars", 5000), 5000)
    timeout_s = float(payload.get("timeout_s", 10.0))
    try:
        return BrowserTools.read_dom(url, max_chars=max_chars, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _browser_extract_links(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_tools import BrowserTools

    url = str(payload.get("url", "")).strip()
    if not url:
        return {"status": "error", "message": "url is required"}
    max_links = _to_int(payload.get("max_links", 50), 50)
    same_domain_only = bool(payload.get("same_domain_only", False))
    timeout_s = float(payload.get("timeout_s", 10.0))
    try:
        return BrowserTools.extract_links(
            url,
            max_links=max_links,
            same_domain_only=same_domain_only,
            timeout_s=timeout_s,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _browser_session_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    return BrowserSessionTools.create_session(payload)


def _browser_session_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    return BrowserSessionTools.list_sessions(payload)


def _browser_session_close(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    session_id = str(payload.get("session_id", "")).strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    return BrowserSessionTools.close_session({"session_id": session_id})


def _browser_session_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    session_id = str(payload.get("session_id", "")).strip()
    url = str(payload.get("url", "")).strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    if not url:
        return {"status": "error", "message": "url is required"}
    return BrowserSessionTools.request(payload)


def _browser_session_read_dom(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    session_id = str(payload.get("session_id", "")).strip()
    url = str(payload.get("url", "")).strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    if not url:
        return {"status": "error", "message": "url is required"}
    return BrowserSessionTools.read_dom(payload)


def _browser_session_extract_links(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.browser_session_tools import BrowserSessionTools

    session_id = str(payload.get("session_id", "")).strip()
    url = str(payload.get("url", "")).strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    if not url:
        return {"status": "error", "message": "url is required"}
    return BrowserSessionTools.extract_links(payload)


def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.vision_tools import VisionTools

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    target = str(payload.get("path", "")).strip() or str(Path.home() / "Pictures" / f"jarvis_observe_{timestamp}.png")

    screenshot_payload: Dict[str, Any] = {"path": target}
    if "region" in payload:
        screenshot_payload["region"] = payload.get("region")
    screenshot_result = _screenshot_capture(screenshot_payload)
    if screenshot_result.get("status") != "success":
        return screenshot_result

    image_path = str(screenshot_result.get("path", ""))
    screen_hash = ""
    hash_error = ""
    try:
        screen_hash = VisionTools.perceptual_hash(image_path)
    except Exception as exc:  # noqa: BLE001
        hash_error = str(exc)

    include_targets = bool(payload.get("include_targets", False))
    min_confidence = float(payload.get("min_confidence", 35.0))
    targets: List[Dict[str, Any]] = []
    targets_error = ""
    if include_targets:
        try:
            targets = VisionTools.extract_text_targets(image_path, min_confidence=min_confidence)
        except Exception as exc:  # noqa: BLE001
            targets_error = str(exc)

    ocr_result = _extract_text_from_image({"path": image_path})
    if ocr_result.get("status") == "success":
        output = {
            "status": "success",
            "screenshot_path": image_path,
            "text": str(ocr_result.get("text", "")),
            "chars": int(ocr_result.get("chars", 0)),
            "ocr_status": "success",
            "screen_hash": screen_hash,
            "targets": targets if include_targets else [],
            "target_count": len(targets) if include_targets else 0,
        }
        if hash_error:
            output["hash_error"] = hash_error
        if targets_error:
            output["targets_error"] = targets_error
        return output

    output = {
        "status": "success",
        "screenshot_path": image_path,
        "text": "",
        "chars": 0,
        "ocr_status": "degraded",
        "ocr_error": ocr_result.get("message", "OCR unavailable"),
        "screen_hash": screen_hash,
        "targets": targets if include_targets else [],
        "target_count": len(targets) if include_targets else 0,
    }
    if hash_error:
        output["hash_error"] = hash_error
    if targets_error:
        output["targets_error"] = targets_error
    return output


def _computer_assert_text_visible(payload: Dict[str, Any]) -> Dict[str, Any]:
    phrase = str(payload.get("text", "")).strip()
    if not phrase:
        return {"status": "error", "message": "text is required"}

    observe_payload: Dict[str, Any] = {}
    if "path" in payload:
        observe_payload["path"] = payload.get("path")
    if "region" in payload:
        observe_payload["region"] = payload.get("region")
    observed = _computer_observe(observe_payload)
    if observed.get("status") != "success":
        return observed

    text = str(observed.get("text", ""))
    found = phrase.lower() in text.lower()
    return {
        "status": "success",
        "text": phrase,
        "found": found,
        "screenshot_path": observed.get("screenshot_path", ""),
        "ocr_status": observed.get("ocr_status", "degraded"),
        "chars": observed.get("chars", 0),
    }


def _computer_find_text_targets(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.vision_tools import VisionTools

    query = str(payload.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "query is required"}

    observe_payload: Dict[str, Any] = {"include_targets": False}
    if "path" in payload:
        observe_payload["path"] = payload.get("path")
    if "region" in payload:
        observe_payload["region"] = payload.get("region")

    observed = _computer_observe(observe_payload)
    if observed.get("status") != "success":
        return observed

    image_path = str(observed.get("screenshot_path", ""))
    match_mode = str(payload.get("match_mode", "contains")).strip().lower() or "contains"
    min_confidence = float(payload.get("min_confidence", 35.0))
    limit = max(1, min(_to_int(payload.get("limit", 20), 20), 200))
    try:
        matches = VisionTools.find_text_targets(
            image_path,
            query=query,
            match_mode=match_mode,
            min_confidence=min_confidence,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}

    return {
        "status": "success",
        "query": query,
        "match_mode": match_mode,
        "min_confidence": min_confidence,
        "screenshot_path": image_path,
        "screen_hash": observed.get("screen_hash", ""),
        "targets": matches,
        "count": len(matches),
    }


def _computer_wait_for_text(payload: Dict[str, Any]) -> Dict[str, Any]:
    phrase = str(payload.get("text", "")).strip()
    if not phrase:
        return {"status": "error", "message": "text is required"}

    timeout_s = max(0.2, min(float(payload.get("timeout_s", 8.0)), 120.0))
    interval_s = max(0.1, min(float(payload.get("interval_s", 0.5)), 10.0))
    expect_visible = bool(payload.get("expect_visible", True))
    deadline = time.time() + timeout_s
    last_seen: Dict[str, Any] = {}

    while time.time() <= deadline:
        observed = _computer_assert_text_visible(payload)
        if observed.get("status") != "success":
            return observed
        found = bool(observed.get("found", False))
        last_seen = observed
        if found is expect_visible:
            return {
                "status": "success",
                "text": phrase,
                "found": found,
                "expect_visible": expect_visible,
                "elapsed_s": round(max(0.0, timeout_s - max(0.0, deadline - time.time())), 3),
                "screenshot_path": observed.get("screenshot_path", ""),
                "ocr_status": observed.get("ocr_status", "degraded"),
            }
        time.sleep(interval_s)

    return {
        "status": "error",
        "message": f"Timeout waiting for text visibility={expect_visible}",
        "text": phrase,
        "found": bool(last_seen.get("found", False)),
        "expect_visible": expect_visible,
        "timeout_s": timeout_s,
        "screenshot_path": str(last_seen.get("screenshot_path", "")),
    }


def _computer_click_text(payload: Dict[str, Any]) -> Dict[str, Any]:
    query = str(payload.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "query is required"}

    target_index = max(0, _to_int(payload.get("target_index", 0), 0))
    button = str(payload.get("button", "left")).strip().lower() or "left"
    clicks = max(1, min(_to_int(payload.get("clicks", 1), 1), 5))
    attempts = max(1, min(_to_int(payload.get("attempts", 4), 4), 12))
    wait_between_s = max(0.05, min(float(payload.get("wait_between_s", 0.45)), 5.0))
    post_wait_s = max(0.0, min(float(payload.get("post_wait_s", 0.2)), 5.0))
    verify_mode = str(payload.get("verify_mode", "changed_or_visible")).strip().lower() or "changed_or_visible"
    min_confidence = float(payload.get("min_confidence", 35.0))
    match_mode = str(payload.get("match_mode", "contains")).strip().lower() or "contains"

    last_error = "No matching text target found."
    for attempt in range(1, attempts + 1):
        find_payload = {
            "query": query,
            "match_mode": match_mode,
            "min_confidence": min_confidence,
            "limit": max(10, target_index + 3),
        }
        if "path" in payload:
            find_payload["path"] = payload.get("path")
        if "region" in payload:
            find_payload["region"] = payload.get("region")
        found = _computer_find_text_targets(find_payload)
        if found.get("status") != "success":
            last_error = str(found.get("message", "find_text_targets failed"))
            time.sleep(wait_between_s)
            continue

        targets = found.get("targets")
        if not isinstance(targets, list) or len(targets) <= target_index:
            last_error = "No matching text target found."
            time.sleep(wait_between_s)
            continue

        target = targets[target_index]
        try:
            x = int(target.get("center_x"))
            y = int(target.get("center_y"))
        except Exception:
            last_error = "Invalid target coordinates."
            time.sleep(wait_between_s)
            continue

        pre_hash = str(found.get("screen_hash", ""))
        move_result = _mouse_move({"x": x, "y": y, "duration": float(payload.get("move_duration", 0.08))})
        if move_result.get("status") != "success":
            last_error = str(move_result.get("message", "mouse move failed"))
            time.sleep(wait_between_s)
            continue

        click_result = _mouse_click(
            {
                "x": x,
                "y": y,
                "button": button,
                "clicks": clicks,
                "interval": float(payload.get("click_interval", 0.05)),
            }
        )
        if click_result.get("status") != "success":
            last_error = str(click_result.get("message", "mouse click failed"))
            time.sleep(wait_between_s)
            continue

        if post_wait_s > 0:
            time.sleep(post_wait_s)
        post_observe_payload: Dict[str, Any] = {}
        if "region" in payload:
            post_observe_payload["region"] = payload.get("region")
        post_observe = _computer_observe(post_observe_payload)
        post_hash = str(post_observe.get("screen_hash", "")) if isinstance(post_observe, dict) else ""
        changed = bool(pre_hash and post_hash and pre_hash != post_hash)

        if verify_mode == "changed" and not changed:
            last_error = "Screen hash did not change after click."
            time.sleep(wait_between_s)
            continue
        if verify_mode == "changed_or_visible":
            visible = _computer_assert_text_visible({"text": query, **({"region": payload.get("region")} if "region" in payload else {})})
            if not changed and (visible.get("status") != "success" or not bool(visible.get("found"))):
                last_error = "Target text not visible and screen hash unchanged after click."
                time.sleep(wait_between_s)
                continue

        return {
            "status": "success",
            "query": query,
            "attempt": attempt,
            "target_index": target_index,
            "target": target,
            "x": x,
            "y": y,
            "button": button,
            "clicks": clicks,
            "pre_hash": pre_hash,
            "post_hash": post_hash,
            "screen_changed": changed,
            "screenshot_path": str(found.get("screenshot_path", "")),
        }

    return {"status": "error", "message": last_error, "query": query, "attempts": attempts}


def _computer_click_target(payload: Dict[str, Any]) -> Dict[str, Any]:
    query = str(payload.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "query is required"}

    target_mode = str(payload.get("target_mode", "auto")).strip().lower() or "auto"
    if target_mode not in {"auto", "accessibility", "ocr"}:
        return {"status": "error", "message": "target_mode must be auto, accessibility, or ocr"}
    verify_mode = str(payload.get("verify_mode", "state_or_visibility")).strip().lower() or "state_or_visibility"
    if verify_mode not in {"state_or_visibility", "hash_changed", "visible", "none"}:
        return {"status": "error", "message": "verify_mode must be state_or_visibility, hash_changed, visible, or none"}

    attempts = max(1, min(_to_int(payload.get("attempts", 3), 3), 10))
    wait_between_s = max(0.05, min(float(payload.get("wait_between_s", 0.45)), 5.0))
    post_wait_s = max(0.0, min(float(payload.get("post_wait_s", 0.2)), 5.0))
    verify_text = str(payload.get("verify_text", "")).strip() or query
    window_title = str(payload.get("window_title", "")).strip()
    control_type = str(payload.get("control_type", "")).strip()
    element_id = str(payload.get("element_id", "")).strip()
    button = str(payload.get("button", "left")).strip().lower() or "left"
    if button not in {"left", "right", "middle"}:
        return {"status": "error", "message": "button must be left, right, or middle"}

    trace: List[Dict[str, Any]] = []
    last_error = "Unable to resolve and click target."
    for attempt in range(1, attempts + 1):
        pre_observe_payload: Dict[str, Any] = {}
        if "region" in payload:
            pre_observe_payload["region"] = payload.get("region")
        pre_observe = _computer_observe(pre_observe_payload)
        pre_hash = str(pre_observe.get("screen_hash", "")) if isinstance(pre_observe, dict) else ""
        resolver: Dict[str, Any] = {"attempt": attempt, "target_mode": target_mode, "pre_hash": pre_hash}

        method = ""
        click_result: Dict[str, Any] = {"status": "error", "message": "no action attempted"}
        if target_mode in {"auto", "accessibility"}:
            invoke_payload: Dict[str, Any] = {
                "query": query,
                "window_title": window_title,
                "control_type": control_type,
            }
            if element_id:
                invoke_payload["element_id"] = element_id
            if button == "right":
                invoke_payload["action"] = "right_click"
            else:
                invoke_payload["action"] = "click"
            acc_result = _accessibility_invoke_element(invoke_payload)
            resolver["accessibility"] = {
                "status": acc_result.get("status"),
                "message": str(acc_result.get("message", "")),
            }
            if acc_result.get("status") == "success":
                method = "accessibility"
                click_result = acc_result
            elif target_mode == "accessibility":
                last_error = str(acc_result.get("message", "Accessibility click failed"))
                trace.append(dict(resolver, last_error=last_error))
                time.sleep(wait_between_s)
                continue

        if not method and target_mode in {"auto", "ocr"}:
            ocr_payload: Dict[str, Any] = {
                "query": query,
                "target_index": max(0, _to_int(payload.get("target_index", 0), 0)),
                "button": button,
                "clicks": max(1, min(_to_int(payload.get("clicks", 1), 1), 5)),
                "attempts": 1,
                "wait_between_s": wait_between_s,
                "post_wait_s": post_wait_s,
                "verify_mode": "changed_or_visible",
                "match_mode": str(payload.get("match_mode", "contains")).strip().lower() or "contains",
                "min_confidence": float(payload.get("min_confidence", 35.0)),
            }
            if "region" in payload:
                ocr_payload["region"] = payload.get("region")
            ocr_result = _computer_click_text(ocr_payload)
            resolver["ocr"] = {
                "status": ocr_result.get("status"),
                "message": str(ocr_result.get("message", "")),
            }
            if ocr_result.get("status") == "success":
                method = "ocr_text"
                click_result = ocr_result
            else:
                last_error = str(ocr_result.get("message", "OCR click failed"))

        if not method or click_result.get("status") != "success":
            if not last_error:
                last_error = str(click_result.get("message", "click failed"))
            trace.append(dict(resolver, last_error=last_error))
            time.sleep(wait_between_s)
            continue

        if post_wait_s > 0:
            time.sleep(post_wait_s)
        post_observe_payload: Dict[str, Any] = {}
        if "region" in payload:
            post_observe_payload["region"] = payload.get("region")
        post_observe = _computer_observe(post_observe_payload)
        post_hash = str(post_observe.get("screen_hash", "")) if isinstance(post_observe, dict) else ""
        changed = bool(pre_hash and post_hash and pre_hash != post_hash)
        visible = False
        if verify_mode in {"state_or_visibility", "visible"} and verify_text:
            visible_check_payload: Dict[str, Any] = {"text": verify_text}
            if "region" in payload:
                visible_check_payload["region"] = payload.get("region")
            visible_result = _computer_assert_text_visible(visible_check_payload)
            visible = bool(visible_result.get("status") == "success" and visible_result.get("found") is True)
            resolver["visibility"] = {
                "status": visible_result.get("status"),
                "found": visible_result.get("found"),
            }

        if verify_mode == "hash_changed" and not changed:
            last_error = "Post-click verification failed: screen hash unchanged."
            trace.append(dict(resolver, method=method, post_hash=post_hash, changed=changed, last_error=last_error))
            time.sleep(wait_between_s)
            continue
        if verify_mode == "visible" and not visible:
            last_error = f"Post-click verification failed: '{verify_text}' is not visible."
            trace.append(dict(resolver, method=method, post_hash=post_hash, changed=changed, visible=visible, last_error=last_error))
            time.sleep(wait_between_s)
            continue
        if verify_mode == "state_or_visibility" and not (changed or visible):
            last_error = "Post-click verification failed: no UI state change and target text not visible."
            trace.append(dict(resolver, method=method, post_hash=post_hash, changed=changed, visible=visible, last_error=last_error))
            time.sleep(wait_between_s)
            continue

        output: Dict[str, Any] = {
            "status": "success",
            "query": query,
            "attempt": attempt,
            "attempts": attempts,
            "method": method,
            "verify_mode": verify_mode,
            "screen_changed": changed,
            "pre_hash": pre_hash,
            "post_hash": post_hash,
            "trace": trace + [dict(resolver, method=method, post_hash=post_hash, changed=changed, visible=visible)],
            "result": click_result,
        }
        if isinstance(click_result, dict):
            for key in ("x", "y", "target", "button", "clicks"):
                if key in click_result and key not in output:
                    output[key] = click_result[key]
        return output

    return {
        "status": "error",
        "message": last_error,
        "query": query,
        "attempts": attempts,
        "trace": trace,
    }


def _extract_text_from_image(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.vision_tools import VisionTools

    target = str(payload.get("path", "")).strip()
    if not target:
        return {"status": "error", "message": "path is required"}

    ok, safe_path = _resolve_safe_path(target)
    if not ok:
        return {"status": "error", "message": str(safe_path)}

    if not Path(safe_path).is_file():
        return {"status": "error", "message": "path must reference an image file"}

    try:
        text = VisionTools.extract_text_from_image(str(safe_path))
        return {"status": "success", "path": str(safe_path), "text": text, "chars": len(text)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _run_whitelisted_app(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.automation_tools import AutomationTools

    app_name = str(payload.get("app_name", "")).strip().lower()
    if not app_name:
        return {"status": "error", "message": "app_name is required"}
    try:
        process = AutomationTools.run_whitelisted_app(app_name)
        return {"status": "success", "app_name": app_name, "pid": int(getattr(process, "pid", 0))}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _run_trusted_script(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.automation_tools import AutomationTools

    script_name = str(payload.get("script_name", "")).strip()
    if not script_name:
        return {"status": "error", "message": "script_name is required"}
    try:
        process = AutomationTools.run_trusted_script(script_name)
        return {"status": "success", "script_name": script_name, "pid": int(getattr(process, "pid", 0))}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _tts_policy_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    source = str(payload.get("source", metadata.get("source", "")) or "").strip().lower() or "unknown"
    mission_id = str(payload.get("mission_id", metadata.get("mission_id", "")) or "").strip()
    policy_profile = str(payload.get("policy_profile", metadata.get("policy_profile", "")) or "").strip().lower()
    risk_level = str(payload.get("risk_level", metadata.get("risk_level", metadata.get("mission_risk_level", ""))) or "").strip().lower()
    return {
        "source": source,
        "mission_id": mission_id,
        "policy_profile": policy_profile,
        "risk_level": risk_level,
        "requires_offline": _to_bool(payload.get("requires_offline", metadata.get("requires_offline", False)), default=False),
        "privacy_mode": _to_bool(payload.get("privacy_mode", metadata.get("privacy_mode", False)), default=False),
    }


def _canonical_tts_provider(raw_provider: str) -> str:
    provider = str(raw_provider or "").strip().lower()
    if provider in {"", "auto"}:
        return "auto"
    if provider in {
        "local",
        "pyttsx3",
        "local-pyttsx3",
        "win32",
        "sapi",
        "win32_sapi",
        "local-win32-sapi",
        "neural",
        "neural_runtime",
        "local-neural",
        "local-neural-runtime",
        "orpheus",
        "command",
        "http",
        "openai_http",
        "coqui",
        "coqui_cli",
        "coqui_python",
        "llama_cpp",
    }:
        return "local"
    if provider in {"elevenlabs", "remote"}:
        return "elevenlabs"
    return provider


def _tts_local_ready(local_payload: Dict[str, Any]) -> bool:
    providers = local_payload.get("providers", {}) if isinstance(local_payload, dict) else {}
    if isinstance(providers, dict) and providers:
        for row in providers.values():
            if not isinstance(row, dict):
                continue
            if bool(row.get("enabled", True)) and bool(row.get("ready", False)):
                return True
        return False
    return str(local_payload.get("status", "")).strip().lower() == "success" if isinstance(local_payload, dict) else False


def _tts_model_route_policy(model_route: Dict[str, Any]) -> Dict[str, Any]:
    route_item = model_route.get("route_item", {}) if isinstance(model_route.get("route_item", {}), dict) else {}
    route_policy = route_item.get("route_policy", {}) if isinstance(route_item.get("route_policy", {}), dict) else {}
    route_selected_provider = str(model_route.get("selected_provider", "")).strip().lower()
    fallback_candidates: List[str] = []
    seen: set[str] = set()
    for item in route_policy.get("cloud_fallback_candidates", []) if isinstance(route_policy.get("cloud_fallback_candidates", []), list) else []:
        clean = _canonical_tts_provider(str(item or "").strip().lower())
        if clean not in {"elevenlabs"} or clean in seen:
            continue
        seen.add(clean)
        fallback_candidates.append(clean)
    recommended_provider = _canonical_tts_provider(str(route_policy.get("recommended_provider", "")).strip().lower())
    if recommended_provider not in {"local", "elevenlabs"}:
        if route_selected_provider in {"local", "elevenlabs"} and not bool(route_item.get("route_blocked", False)):
            recommended_provider = route_selected_provider
        elif fallback_candidates:
            recommended_provider = fallback_candidates[0]
        else:
            recommended_provider = ""
    return {
        "route_item": dict(route_item),
        "route_policy": dict(route_policy),
        "selected_provider": route_selected_provider,
        "recommended_provider": recommended_provider,
        "route_adjusted": bool(route_item.get("route_adjusted", False)),
        "route_blocked": bool(route_item.get("route_blocked", False)),
        "route_warning": str(route_item.get("route_warning", "")).strip(),
        "reason_code": str(route_policy.get("reason_code", route_item.get("route_adjustment_reason", ""))).strip().lower(),
        "reason": str(route_policy.get("reason", "")).strip(),
        "blacklisted": bool(route_policy.get("blacklisted", False)),
        "review_required": bool(route_policy.get("review_required", False)),
        "autonomy_safe": bool(route_policy.get("autonomy_safe", False)),
        "autonomous_allowed": bool(route_policy.get("autonomous_allowed", True)),
        "local_route_viable": bool(route_policy.get("local_route_viable", route_selected_provider == "local")),
        "recovery_pending": bool(route_policy.get("recovery_pending", False)),
        "cooldown_hint_s": max(0, _to_int(route_policy.get("cooldown_hint_s", 0), 0)),
        "cloud_fallback_candidates": list(fallback_candidates),
    }


def _apply_tts_route_policy(
    *,
    provider_chain: List[str],
    requested_provider: str,
    context: Dict[str, Any],
    model_route: Dict[str, Any],
    availability: Dict[str, bool],
) -> Tuple[List[str], Dict[str, Any]]:
    chain = [str(item or "").strip().lower() for item in provider_chain if str(item or "").strip()]
    policy_meta = _tts_model_route_policy(model_route)
    route_selected_provider = str(policy_meta.get("selected_provider", "")).strip().lower()
    recommended_provider = str(policy_meta.get("recommended_provider", "")).strip().lower()
    force_local = bool(context.get("requires_offline", False) or context.get("privacy_mode", False))
    route_blocked = bool(policy_meta.get("route_blocked", False))
    blacklisted = bool(policy_meta.get("blacklisted", False))
    local_route_viable = bool(policy_meta.get("local_route_viable", True))
    desired_provider = ""
    reason_suffix = ""
    removed_local = False

    if force_local:
        if availability.get("local", False):
            desired_provider = "local"
            reason_suffix = "route_policy_force_local"
    elif route_selected_provider in {"local", "elevenlabs"} and requested_provider == "auto" and availability.get(route_selected_provider, False):
        desired_provider = route_selected_provider
        reason_suffix = "model_route"
    if route_blocked and recommended_provider in {"elevenlabs"} and availability.get(recommended_provider, False):
        desired_provider = recommended_provider
        reason_suffix = "route_policy_blocked"
    elif blacklisted and recommended_provider in {"elevenlabs"} and availability.get(recommended_provider, False):
        desired_provider = recommended_provider
        reason_suffix = "route_policy_blacklisted"
    elif (
        not force_local
        and requested_provider == "auto"
        and recommended_provider in {"elevenlabs"}
        and availability.get(recommended_provider, False)
        and not bool(policy_meta.get("autonomy_safe", False))
    ):
        desired_provider = recommended_provider
        reason_suffix = "route_policy_preferred"

    if desired_provider:
        chain = [desired_provider] + [item for item in chain if item != desired_provider]

    if not force_local and desired_provider == "elevenlabs" and availability.get("elevenlabs", False):
        if route_blocked or blacklisted or not local_route_viable:
            chain = [item for item in chain if item != "local"]
            removed_local = True

    seen: set[str] = set()
    normalized_chain = [
        item
        for item in chain
        if item in {"local", "elevenlabs"} and availability.get(item, False) and not (item in seen or seen.add(item))
    ]
    if not normalized_chain:
        normalized_chain = [
            item for item in ("local", "elevenlabs") if availability.get(item, False)
        ]

    policy_meta["execution_chain"] = list(normalized_chain)
    policy_meta["execution_removed_local"] = removed_local
    policy_meta["execution_force_local"] = force_local
    policy_meta["execution_reason"] = reason_suffix
    return normalized_chain, policy_meta


def _append_tts_route_policy_hints(
    remediation_hints: List[Dict[str, Any]],
    *,
    route_policy: Dict[str, Any],
    configured_remote: bool,
) -> None:
    reason_code = str(route_policy.get("reason_code", "")).strip().lower()
    reason = str(route_policy.get("reason", "")).strip()
    recommended_provider = str(route_policy.get("recommended_provider", "")).strip().lower()
    route_blocked = bool(route_policy.get("route_blocked", False))
    route_adjusted = bool(route_policy.get("route_adjusted", False))
    blacklisted = bool(route_policy.get("blacklisted", False))
    cooldown_hint_s = max(0, _to_int(route_policy.get("cooldown_hint_s", 0), 0))
    fallback_candidates = [
        str(item or "").strip().lower()
        for item in route_policy.get("cloud_fallback_candidates", [])
        if str(item or "").strip()
    ] if isinstance(route_policy.get("cloud_fallback_candidates", []), list) else []

    if route_adjusted and recommended_provider:
        remediation_hints.append(
            {
                "code": "tts_route_policy_rerouted",
                "severity": "low",
                "message": reason or f"TTS execution was rerouted to {recommended_provider} due to local launcher policy.",
                "recommended_provider": recommended_provider,
                "reason_code": reason_code,
            }
        )
    if route_blocked:
        remediation_hints.append(
            {
                "code": "tts_route_policy_blocked",
                "severity": "high" if not configured_remote else "medium",
                "message": reason or "Local TTS route is currently blocked by launch policy.",
                "recommended_provider": recommended_provider,
                "fallback_candidates": fallback_candidates,
                "reason_code": reason_code,
            }
        )
    elif blacklisted:
        remediation_hints.append(
            {
                "code": "tts_local_route_blacklisted",
                "severity": "medium",
                "message": reason or "Local TTS route is blacklisted due to launch instability.",
                "recommended_provider": recommended_provider,
                "reason_code": reason_code,
            }
        )
    if cooldown_hint_s > 0:
        remediation_hints.append(
            {
                "code": "tts_route_policy_cooldown",
                "severity": "low",
                "message": f"Local TTS route is in recovery for about {cooldown_hint_s}s.",
                "cooldown_hint_s": cooldown_hint_s,
                "reason_code": reason_code,
            }
        )


def _shared_model_router(*, force_refresh: bool = False) -> Tuple[Any, Any, Any]:
    now = time.monotonic()
    with _MODEL_ROUTER_CACHE_LOCK:
        manager = _MODEL_ROUTER_CACHE.get("manager")
        registry = _MODEL_ROUTER_CACHE.get("registry")
        router = _MODEL_ROUTER_CACHE.get("router")
        created_at = float(_MODEL_ROUTER_CACHE.get("created_at", 0.0) or 0.0)
        if (
            not force_refresh
            and manager is not None
            and registry is not None
            and router is not None
            and (now - created_at) <= 20.0
        ):
            try:
                manager.refresh(overwrite_env=False)
                registry.refresh_environment(force=False)
            except Exception:
                pass
            return manager, registry, router

        from backend.python.core.provider_credentials import ProviderCredentialManager
        from backend.python.inference.model_registry import ModelRegistry
        from backend.python.inference.model_router import ModelRouter

        manager = ProviderCredentialManager()
        manager.refresh(overwrite_env=False)
        registry = ModelRegistry(
            provider_credentials=manager,
            enforce_provider_keys=True,
            scan_local_models=True,
            refresh_interval_s=20.0,
        )
        router = ModelRouter(registry)
        _MODEL_ROUTER_CACHE["created_at"] = now
        _MODEL_ROUTER_CACHE["manager"] = manager
        _MODEL_ROUTER_CACHE["registry"] = registry
        _MODEL_ROUTER_CACHE["router"] = router
        return manager, registry, router


def _resolve_tts_model_route(payload: Dict[str, Any], *, requested_provider: str) -> Dict[str, Any]:
    context = _tts_policy_context(payload)
    clean_profile = str(context.get("policy_profile", "") or "balanced").strip().lower() or "balanced"
    canonical_requested = _canonical_tts_provider(requested_provider)
    preferred_map = {"tts": canonical_requested} if canonical_requested in {"local", "elevenlabs"} else None
    max_cost_units: float | None = None
    raw_max_cost = payload.get("max_cost_units")
    if raw_max_cost not in {None, ""}:
        try:
            max_cost_units = float(raw_max_cost)
        except Exception:  # noqa: BLE001
            max_cost_units = None

    try:
        manager, registry, router = _shared_model_router(force_refresh=False)
        provider_credentials = manager.snapshot() if hasattr(manager, "snapshot") else {}
        route_bundle = router.route_bundle(
            stack_name="voice",
            tasks=["tts"],
            requires_offline=bool(context.get("requires_offline", False)),
            privacy_mode=bool(context.get("privacy_mode", False)),
            latency_sensitive=_to_bool(payload.get("latency_sensitive", False), default=False),
            mission_profile=clean_profile,
            cost_sensitive=_to_bool(payload.get("cost_sensitive", False), default=False),
            max_cost_units=max_cost_units,
            preferred_providers=preferred_map,
        )
        route_item = {}
        if isinstance(route_bundle, dict):
            for row in route_bundle.get("items", []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("task", "")).strip().lower() != "tts":
                    continue
                if str(row.get("status", "")).strip().lower() != "success":
                    continue
                route_item = dict(row)
                break
        selected_provider = str(route_item.get("provider", "")).strip().lower()
        selected_local_path = str(route_item.get("selected_path", "")).strip()
        if selected_local_path and not str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", "")).strip():
            os.environ["JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH"] = selected_local_path
        provider_status = registry.provider_status_snapshot() if hasattr(registry, "provider_status_snapshot") else {}
        return {
            "status": "success",
            "requested_provider": canonical_requested,
            "selected_provider": selected_provider or (canonical_requested if canonical_requested in {"local", "elevenlabs"} else ""),
            "selected_model": str(route_item.get("model", "")).strip(),
            "selected_local_path": selected_local_path,
            "route_item": route_item,
            "route_bundle": route_bundle if isinstance(route_bundle, dict) else {},
            "provider_status": provider_status if isinstance(provider_status, dict) else {},
            "provider_credentials": provider_credentials if isinstance(provider_credentials, dict) else {},
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "message": str(exc),
            "requested_provider": canonical_requested,
            "selected_provider": canonical_requested if canonical_requested in {"local", "elevenlabs"} else "",
            "selected_model": "",
            "selected_local_path": "",
            "route_item": {},
            "route_bundle": {},
            "provider_status": {},
            "provider_credentials": {},
        }


def _merge_tts_provider_chain(provider_chain: List[str], *, selected_provider: str) -> List[str]:
    clean_selected = str(selected_provider or "").strip().lower()
    chain = [str(item or "").strip().lower() for item in provider_chain if str(item or "").strip()]
    if clean_selected in {"local", "elevenlabs"}:
        chain = [clean_selected] + [item for item in chain if item != clean_selected]
    seen: set[str] = set()
    return [item for item in chain if item in {"local", "elevenlabs"} and not (item in seen or seen.add(item))]


def _tts_policy_payload(
    *,
    policy_manager: Any,
    policy_decision: Dict[str, Any],
    provider_chain: List[str],
    model_route: Dict[str, Any],
) -> Dict[str, Any]:
    scores = policy_decision.get("scores", {}) if isinstance(policy_decision.get("scores", {}), dict) else {}
    route_item = model_route.get("route_item", {}) if isinstance(model_route.get("route_item", {}), dict) else {}
    route_policy = route_item.get("route_policy", {}) if isinstance(route_item.get("route_policy", {}), dict) else {}
    return {
        "applied": policy_manager is not None,
        "decision": {
            "selected_provider": str(policy_decision.get("selected_provider", provider_chain[0] if provider_chain else "")),
            "chain": list(provider_chain),
            "reason": str(policy_decision.get("reason", "")),
            "scores": dict(scores),
            "route_policy": dict(policy_decision.get("route_policy", {}))
            if isinstance(policy_decision.get("route_policy", {}), dict)
            else {},
        },
        "model_route": {
            "status": str(model_route.get("status", "")),
            "selected_provider": str(model_route.get("selected_provider", "")),
            "selected_model": str(model_route.get("selected_model", "")),
            "selected_local_path": str(model_route.get("selected_local_path", "")),
            "route_item": dict(route_item),
            "route_policy": dict(route_policy),
            "route_adjusted": bool(route_item.get("route_adjusted", False)),
            "route_blocked": bool(route_item.get("route_blocked", False)),
            "route_warning": str(route_item.get("route_warning", "")),
        },
    }


def _tts_speak(payload: Dict[str, Any]) -> Dict[str, Any]:
    text = str(payload.get("text", "")).strip()
    if not text:
        return {"status": "error", "message": "Text is required."}

    requested_provider = str(payload.get("provider", payload.get("tts_provider", "auto")) or "auto").strip().lower() or "auto"
    supported = {
        "auto",
        "local",
        "neural",
        "neural_runtime",
        "local-neural",
        "local-neural-runtime",
        "orpheus",
        "pyttsx3",
        "local-pyttsx3",
        "win32",
        "sapi",
        "win32_sapi",
        "local-win32-sapi",
        "command",
        "http",
        "openai_http",
        "coqui",
        "coqui_cli",
        "coqui_python",
        "llama_cpp",
        "elevenlabs",
        "remote",
    }
    if requested_provider not in supported:
        return {"status": "error", "message": f"Unsupported provider '{requested_provider}'."}

    context = _tts_policy_context(payload)
    attempts: List[Dict[str, Any]] = []
    failures: List[str] = []
    policy_decision: Dict[str, Any] = {}
    model_route = _resolve_tts_model_route(payload, requested_provider=requested_provider)
    route_selected_provider = str(model_route.get("selected_provider", "")).strip().lower()
    selected_local_path = str(model_route.get("selected_local_path", "")).strip()
    if selected_local_path and not str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", "")).strip():
        os.environ["JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH"] = selected_local_path

    api_key = os.getenv("ELEVENLABS_API_KEY")
    voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
    if not api_key:
        try:
            from backend.python.core.provider_credentials import ProviderCredentialManager

            manager = ProviderCredentialManager()
            manager.refresh(overwrite_env=False)
            api_key = manager.get_api_key("elevenlabs")
        except Exception:
            api_key = api_key or ""
    elevenlabs_ready = bool(api_key and voice_id)
    local_voice = str(payload.get("voice", os.getenv("JARVIS_LOCAL_TTS_VOICE", ""))).strip()
    local_rate = _to_int(payload.get("rate", os.getenv("JARVIS_LOCAL_TTS_RATE", "175")), 175)
    local_volume_raw = payload.get("volume", os.getenv("JARVIS_LOCAL_TTS_VOLUME", "1.0"))
    try:
        local_volume = float(local_volume_raw)
    except Exception:  # noqa: BLE001
        local_volume = 1.0

    provider_chain: List[str] = []
    policy_manager: Any = None
    try:
        from backend.python.speech.tts_policy import TtsPolicyManager

        local_available = True
        try:
            from backend.python.speech.local_tts import LocalTTS

            local_available = _tts_local_ready(LocalTTS.diagnostics(history_limit=6))
        except Exception:
            local_available = True
        policy_manager = TtsPolicyManager.shared()
        policy_decision = policy_manager.choose_provider(
            requested_provider=requested_provider,
            availability={"local": local_available, "elevenlabs": elevenlabs_ready},
            context=context,
        )
        provider_chain = [str(item).strip().lower() for item in policy_decision.get("chain", []) if str(item).strip()]
    except Exception:
        policy_manager = None
        policy_decision = {}

    if not provider_chain:
        if requested_provider in {"elevenlabs", "remote"}:
            provider_chain = ["elevenlabs", "local"]
        elif requested_provider in {
            "local",
            "neural",
            "neural_runtime",
            "local-neural",
            "local-neural-runtime",
            "orpheus",
            "pyttsx3",
            "local-pyttsx3",
            "win32",
            "sapi",
            "win32_sapi",
            "local-win32-sapi",
            "command",
            "http",
            "openai_http",
            "coqui",
            "coqui_cli",
            "coqui_python",
            "llama_cpp",
        }:
            provider_chain = ["local", "elevenlabs"]
        else:
            provider_chain = ["elevenlabs", "local"] if elevenlabs_ready else ["local", "elevenlabs"]

    provider_chain = _merge_tts_provider_chain(
        provider_chain,
        selected_provider=route_selected_provider if requested_provider == "auto" else "",
    )
    provider_chain, route_policy = _apply_tts_route_policy(
        provider_chain=provider_chain,
        requested_provider=requested_provider,
        context=context,
        model_route=model_route,
        availability={"local": local_available, "elevenlabs": elevenlabs_ready},
    )
    if not provider_chain:
        provider_chain = ["local"]
    if isinstance(policy_decision, dict):
        policy_decision = dict(policy_decision)
        policy_decision["chain"] = list(provider_chain)
        if provider_chain:
            policy_decision["selected_provider"] = provider_chain[0]
        policy_decision["route_policy"] = dict(route_policy)
        reason_suffix = str(route_policy.get("execution_reason", "")).strip()
        if requested_provider == "auto" and route_selected_provider in {"local", "elevenlabs"}:
            base_reason = str(policy_decision.get("reason", "")).strip()
            reason_value = reason_suffix or "model_route"
            policy_decision["reason"] = f"{base_reason}|{reason_value}" if base_reason else reason_value
        elif reason_suffix:
            base_reason = str(policy_decision.get("reason", "")).strip()
            policy_decision["reason"] = f"{base_reason}|{reason_suffix}" if base_reason else reason_suffix

    for provider in provider_chain:
        if provider == "elevenlabs":
            if not elevenlabs_ready:
                row = {
                    "provider": "elevenlabs",
                    "status": "skipped",
                    "reason": "not_configured",
                    "message": "ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID are required.",
                }
                attempts.append(row)
                if requested_provider in {"elevenlabs", "remote"}:
                    failures.append("ElevenLabs is not configured (missing API key or voice id).")
                if policy_manager is not None:
                    try:
                        policy_manager.record_attempt(
                            provider="elevenlabs",
                            status="skipped",
                            message=str(row.get("message", "")),
                            context=context,
                            decision=policy_decision,
                            attempt_index=len(attempts),
                        )
                    except Exception:
                        pass
                continue

            started = time.monotonic()
            try:
                from backend.python.speech.elevenlabs_tts import ElevenLabsTTS

                cloud_result = ElevenLabsTTS(api_key=api_key, voice_id=voice_id).speak(text)
            except Exception as exc:  # noqa: BLE001
                cloud_result = {"status": "error", "message": str(exc), "mode": "elevenlabs"}
            latency_s = max(0.0, time.monotonic() - started)
            status_name = str(cloud_result.get("status", "error")).lower()
            message = str(cloud_result.get("message", "")).strip()
            attempts.append(
                {
                    "provider": "elevenlabs",
                    "status": status_name,
                    "latency_s": round(latency_s, 4),
                    "message": message,
                }
            )
            if policy_manager is not None:
                try:
                    policy_manager.record_attempt(
                        provider="elevenlabs",
                        status=status_name,
                        latency_s=latency_s,
                        message=message,
                        context=context,
                        decision=policy_decision,
                        attempt_index=len(attempts),
                    )
                except Exception:
                    pass
            if cloud_result.get("status") == "success":
                cloud_result["requested_provider"] = requested_provider
                cloud_result["attempts"] = attempts
                cloud_result["model_route"] = _tts_policy_payload(
                    policy_manager=policy_manager,
                    policy_decision=policy_decision,
                    provider_chain=provider_chain,
                    model_route=model_route,
                )["model_route"]
                cloud_result["policy"] = _tts_policy_payload(
                    policy_manager=policy_manager,
                    policy_decision=policy_decision,
                    provider_chain=provider_chain,
                    model_route=model_route,
                )
                return cloud_result
            failures.append(message or "ElevenLabs failed")
            continue

        if provider == "local":
            started = time.monotonic()
            try:
                from backend.python.speech.local_tts import LocalTTS

                local_pref = "auto" if requested_provider in {"auto", "elevenlabs", "remote"} else requested_provider
                local_result = LocalTTS(voice=local_voice, rate=local_rate, volume=local_volume).speak(
                    text,
                    provider_preference=local_pref,
                )
            except Exception as exc:  # noqa: BLE001
                local_result = {"status": "error", "message": str(exc)}
            latency_s = max(0.0, time.monotonic() - started)
            status_name = str(local_result.get("status", "error")).lower()
            message = str(local_result.get("message", "")).strip()
            attempts.append(
                {
                    "provider": "local",
                    "status": status_name,
                    "latency_s": round(latency_s, 4),
                    "provider_used": str(local_result.get("provider_used", "")).strip(),
                    "message": message,
                }
            )
            if policy_manager is not None:
                try:
                    policy_manager.record_attempt(
                        provider="local",
                        status=status_name,
                        latency_s=latency_s,
                        message=message,
                        context=context,
                        decision=policy_decision,
                        attempt_index=len(attempts),
                    )
                except Exception:
                    pass
            if local_result.get("status") == "success":
                local_result["requested_provider"] = requested_provider
                local_result["attempts"] = attempts
                if selected_local_path:
                    local_result["selected_local_model_path"] = selected_local_path
                local_result["model_route"] = _tts_policy_payload(
                    policy_manager=policy_manager,
                    policy_decision=policy_decision,
                    provider_chain=provider_chain,
                    model_route=model_route,
                )["model_route"]
                local_result["policy"] = _tts_policy_payload(
                    policy_manager=policy_manager,
                    policy_decision=policy_decision,
                    provider_chain=provider_chain,
                    model_route=model_route,
                )
                return local_result
            failures.append(message or "Local TTS failed")

    allow_text_fallback = _to_bool(
        payload.get("allow_text_fallback", os.getenv("JARVIS_TTS_ALLOW_TEXT_FALLBACK", "1")),
        default=True,
    )
    if allow_text_fallback:
        return {
            "status": "success",
            "text": text,
            "mode": "fallback-text",
            "requested_provider": requested_provider,
            "attempts": attempts,
            "model_route": _tts_policy_payload(
                policy_manager=policy_manager,
                policy_decision=policy_decision,
                provider_chain=provider_chain,
                model_route=model_route,
            )["model_route"],
            "policy": _tts_policy_payload(
                policy_manager=policy_manager,
                policy_decision=policy_decision,
                provider_chain=provider_chain,
                model_route=model_route,
            ),
            "message": "No TTS provider succeeded; returning text fallback.",
        }
    return {
        "status": "error",
        "message": "; ".join([item for item in failures if item]) or "No TTS provider succeeded.",
        "requested_provider": requested_provider,
        "attempts": attempts,
        "model_route": _tts_policy_payload(
            policy_manager=policy_manager,
            policy_decision=policy_decision,
            provider_chain=provider_chain,
            model_route=model_route,
        )["model_route"],
        "policy": _tts_policy_payload(
            policy_manager=policy_manager,
            policy_decision=policy_decision,
            provider_chain=provider_chain,
            model_route=model_route,
        ),
    }


def _tts_stop(payload: Dict[str, Any]) -> Dict[str, Any]:
    clean_payload = payload if isinstance(payload, dict) else {}
    target_session = str(clean_payload.get("session_id", "")).strip()
    target_provider = str(clean_payload.get("provider", "")).strip().lower()
    responses: list[Dict[str, Any]] = []
    api_key = os.getenv("ELEVENLABS_API_KEY")
    voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
    if not api_key:
        try:
            from backend.python.core.provider_credentials import ProviderCredentialManager

            manager = ProviderCredentialManager()
            manager.refresh(overwrite_env=False)
            api_key = manager.get_api_key("elevenlabs")
        except Exception:
            api_key = api_key or ""
    elevenlabs_selected = target_provider in {"", "elevenlabs", "remote"}
    local_selected = target_provider in {
        "",
        "local",
        "pyttsx3",
        "local-pyttsx3",
        "win32",
        "sapi",
        "win32_sapi",
        "local-win32-sapi",
    }
    if target_provider and not elevenlabs_selected and not local_selected:
        return {"status": "error", "stopped": False, "message": f"Unsupported provider '{target_provider}'."}
    if elevenlabs_selected and api_key and voice_id:
        try:
            from backend.python.speech.elevenlabs_tts import ElevenLabsTTS

            responses.append(ElevenLabsTTS.stop(session_id=target_session))
        except Exception as exc:  # noqa: BLE001
            responses.append({"status": "error", "stopped": False, "message": str(exc), "mode": "elevenlabs"})
    if local_selected:
        try:
            from backend.python.speech.local_tts import LocalTTS

            responses.append(LocalTTS.stop(session_id=target_session))
        except Exception as exc:  # noqa: BLE001
            responses.append({"status": "error", "stopped": False, "message": str(exc), "mode": "local"})

    stopped = any(bool(row.get("stopped", False)) for row in responses if isinstance(row, dict))
    if stopped:
        return {"status": "success", "stopped": True, "session_id": target_session, "provider": target_provider, "results": responses}

    errors = [str(row.get("message", "")).strip() for row in responses if isinstance(row, dict) and str(row.get("status", "")).lower() == "error"]
    return {
        "status": "success" if not errors else "error",
        "stopped": False,
        "message": "No active TTS playback." if not errors else "; ".join([item for item in errors if item]),
        "session_id": target_session,
        "provider": target_provider,
        "results": responses,
    }


def _tts_diagnostics(payload: Dict[str, Any]) -> Dict[str, Any]:
    bounded = max(1, min(_to_int(payload.get("history_limit", 24), 24), 200))
    providers: Dict[str, Dict[str, Any]] = {}
    model_route = _resolve_tts_model_route(payload, requested_provider=str(payload.get("provider", "auto")))
    route_bundle = dict(model_route.get("route_bundle", {})) if isinstance(model_route.get("route_bundle", {}), dict) else {}

    local_payload: Dict[str, Any]
    try:
        from backend.python.speech.local_tts import LocalTTS

        local_payload = LocalTTS.diagnostics(history_limit=bounded)
    except Exception as exc:  # noqa: BLE001
        local_payload = {"status": "error", "message": str(exc)}
    providers["local"] = local_payload

    api_key = os.getenv("ELEVENLABS_API_KEY")
    voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
    if not api_key:
        try:
            from backend.python.core.provider_credentials import ProviderCredentialManager

            manager = ProviderCredentialManager()
            manager.refresh(overwrite_env=False)
            api_key = manager.get_api_key("elevenlabs")
        except Exception:
            api_key = api_key or ""
    eleven_configured = bool(api_key and voice_id)

    eleven_payload: Dict[str, Any]
    try:
        from backend.python.speech.elevenlabs_tts import ElevenLabsTTS

        eleven_payload = ElevenLabsTTS.diagnostics()
    except Exception as exc:  # noqa: BLE001
        eleven_payload = {"status": "error", "message": str(exc)}
    eleven_payload["configured"] = eleven_configured
    eleven_payload["has_api_key"] = bool(api_key)
    eleven_payload["has_voice_id"] = bool(voice_id)
    providers["elevenlabs"] = eleven_payload

    local_ready = _tts_local_ready(local_payload)
    local_provider_rows = local_payload.get("providers", {}) if isinstance(local_payload.get("providers", {}), dict) else {}
    neural_payload = local_provider_rows.get("neural_runtime", {}) if isinstance(local_provider_rows, dict) else {}

    recommended_provider = "local"
    if eleven_configured and bool(eleven_payload.get("ready", True)) and not local_ready:
        recommended_provider = "elevenlabs"
    elif not local_ready:
        if eleven_configured:
            recommended_provider = "elevenlabs"

    remediation_hints: List[Dict[str, Any]] = []
    if not eleven_configured:
        remediation_hints.append(
            {
                "code": "configure_elevenlabs_credentials",
                "severity": "medium",
                "message": "Set ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID to enable cloud TTS fallback.",
                "env": ["ELEVENLABS_API_KEY", "ELEVENLABS_VOICE_ID"],
            }
        )
    if not local_ready:
        remediation_hints.append(
            {
                "code": "local_tts_cooldown_active",
                "severity": "low",
                "message": "Local TTS backend is in cooldown due to recent failures; retry shortly.",
            }
        )
    if isinstance(neural_payload, dict) and bool(neural_payload.get("configured", False)) and not bool(neural_payload.get("ready", False)):
        remediation_hints.append(
            {
                "code": "local_neural_tts_unavailable",
                "severity": "medium",
                "message": str(neural_payload.get("message", "Local neural TTS is configured but not ready.")).strip()
                or "Local neural TTS is configured but not ready.",
                "env": [
                    "JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT",
                    "JARVIS_LOCAL_NEURAL_TTS_COMMAND",
                    "JARVIS_LOCAL_NEURAL_TTS_MODEL_NAME",
                    "JARVIS_LOCAL_NEURAL_TTS_CONFIG_PATH",
                ],
            }
        )
    if not eleven_configured and not local_ready:
        remediation_hints.append(
            {
                "code": "tts_unavailable",
                "severity": "high",
                "message": "No TTS provider is currently ready; validate local dependencies and cloud credentials.",
            }
        )
    route_policy = _tts_model_route_policy(model_route)
    _append_tts_route_policy_hints(
        remediation_hints,
        route_policy=route_policy,
        configured_remote=eleven_configured,
    )

    context = _tts_policy_context(payload)
    policy_payload: Dict[str, Any] = {"status": "unavailable"}
    try:
        from backend.python.speech.tts_policy import TtsPolicyManager

        policy_payload = TtsPolicyManager.shared().status(
            limit=bounded,
            context=context,
            availability={"local": local_ready, "elevenlabs": eleven_configured and bool(eleven_payload.get("ready", True))},
        )
        candidate = str(policy_payload.get("recommended_provider", "")).strip().lower()
        if candidate in {"local", "elevenlabs"}:
            recommended_provider = candidate
    except Exception as exc:  # noqa: BLE001
        policy_payload = {"status": "error", "message": str(exc)}

    route_selected_provider = str(model_route.get("selected_provider", "")).strip().lower()
    route_selected_path = str(model_route.get("selected_local_path", "")).strip()
    route_policy_recommended = str(route_policy.get("recommended_provider", "")).strip().lower()
    if route_policy_recommended in {"local", "elevenlabs"}:
        if route_policy_recommended != "elevenlabs" or eleven_configured:
            recommended_provider = route_policy_recommended
    if route_selected_provider in {"local", "elevenlabs"} and context.get("requires_offline") in {True}:
        recommended_provider = route_selected_provider
    elif route_selected_provider in {"local", "elevenlabs"} and context.get("privacy_mode") in {True}:
        recommended_provider = route_selected_provider

    if route_selected_path and isinstance(providers.get("local"), dict):
        providers["local"]["selected_model_path"] = route_selected_path

    return {
        "status": "success",
        "history_limit": bounded,
        "providers": providers,
        "recommended_provider": recommended_provider,
        "remediation_hints": remediation_hints,
        "policy": policy_payload,
        "model_route": {
            "status": str(model_route.get("status", "")),
            "selected_provider": route_selected_provider,
            "selected_model": str(model_route.get("selected_model", "")),
            "selected_local_path": route_selected_path,
            "route_item": dict(model_route.get("route_item", {})) if isinstance(model_route.get("route_item", {}), dict) else {},
            "route_policy": dict(route_policy.get("route_policy", {})) if isinstance(route_policy.get("route_policy", {}), dict) else {},
            "route_adjusted": bool(route_policy.get("route_adjusted", False)),
            "route_blocked": bool(route_policy.get("route_blocked", False)),
            "route_warning": str(route_policy.get("route_warning", "")),
        },
        "route_bundle": route_bundle,
        "route_policy_summary": (
            dict(route_bundle.get("launch_policy_summary", {}))
            if isinstance(route_bundle.get("launch_policy_summary", {}), dict)
            else {}
        ),
        "provider_credentials": dict(model_route.get("provider_credentials", {}))
        if isinstance(model_route.get("provider_credentials", {}), dict)
        else {},
    }


def _tts_policy_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    bounded = max(1, min(_to_int(payload.get("limit", payload.get("history_limit", 120)), 120), 2000))
    context = _tts_policy_context(payload)
    try:
        from backend.python.speech.tts_policy import TtsPolicyManager
        from backend.python.speech.local_tts import LocalTTS

        api_key = os.getenv("ELEVENLABS_API_KEY")
        voice_id = str(os.getenv("ELEVENLABS_VOICE_ID", "") or "").strip()
        if not api_key:
            try:
                from backend.python.core.provider_credentials import ProviderCredentialManager

                manager = ProviderCredentialManager()
                manager.refresh(overwrite_env=False)
                api_key = manager.get_api_key("elevenlabs")
            except Exception:
                api_key = api_key or ""
        elevenlabs_ready = bool(api_key and voice_id)
        try:
            local_payload = LocalTTS.diagnostics(history_limit=min(24, bounded))
        except Exception as exc:  # noqa: BLE001
            local_payload = {"status": "error", "message": str(exc)}
        local_ready = _tts_local_ready(local_payload)
        model_route = _resolve_tts_model_route(payload, requested_provider=str(payload.get("provider", "auto")))
        route_bundle = dict(model_route.get("route_bundle", {})) if isinstance(model_route.get("route_bundle", {}), dict) else {}
        route_policy = _tts_model_route_policy(model_route)
        route_policy_recommended = str(route_policy.get("recommended_provider", "")).strip().lower()
        force_local = bool(context.get("requires_offline", False) or context.get("privacy_mode", False))
        local_available = bool(local_ready)
        if not force_local and (
            bool(route_policy.get("route_blocked", False))
            or bool(route_policy.get("blacklisted", False))
            or not bool(route_policy.get("local_route_viable", True))
        ):
            local_available = False
        status_payload = TtsPolicyManager.shared().status(
            limit=bounded,
            context=context,
            availability={"local": local_available, "elevenlabs": elevenlabs_ready},
        )
        if isinstance(status_payload, dict):
            if route_policy_recommended in {"local", "elevenlabs"}:
                if route_policy_recommended != "elevenlabs" or elevenlabs_ready:
                    status_payload["recommended_provider"] = route_policy_recommended
                    alternate = "local" if route_policy_recommended == "elevenlabs" else "elevenlabs"
                    status_payload["recommended_chain"] = [
                        route_policy_recommended,
                        alternate,
                    ]
            status_payload["model_route"] = {
                "status": str(model_route.get("status", "")),
                "selected_provider": str(model_route.get("selected_provider", "")),
                "selected_model": str(model_route.get("selected_model", "")),
                "selected_local_path": str(model_route.get("selected_local_path", "")),
                "route_item": dict(model_route.get("route_item", {}))
                if isinstance(model_route.get("route_item", {}), dict)
                else {},
                "route_policy": dict(route_policy.get("route_policy", {}))
                if isinstance(route_policy.get("route_policy", {}), dict)
                else {},
                "route_adjusted": bool(route_policy.get("route_adjusted", False)),
                "route_blocked": bool(route_policy.get("route_blocked", False)),
                "route_warning": str(route_policy.get("route_warning", "")),
            }
            status_payload["route_bundle"] = route_bundle
            status_payload["route_policy_summary"] = (
                dict(route_bundle.get("launch_policy_summary", {}))
                if isinstance(route_bundle.get("launch_policy_summary", {}), dict)
                else {}
            )
        return status_payload
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _tts_policy_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else payload
    if not isinstance(config, dict):
        return {"status": "error", "message": "config must be a JSON object"}
    try:
        from backend.python.speech.tts_policy import TtsPolicyManager

        return TtsPolicyManager.shared().update(config)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def _external_connector_status(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.connector_status()


def _external_connector_preflight(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.connector_preflight(payload)


def _external_email_send(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.send_email(payload)


def _external_email_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.list_emails(payload)


def _external_email_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.read_email(payload)


def _external_calendar_create_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.create_calendar_event(payload)


def _external_calendar_list_events(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.list_calendar_events(payload)


def _external_calendar_update_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.update_calendar_event(payload)


def _external_doc_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.create_document(payload)


def _external_doc_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.list_documents(payload)


def _external_doc_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.read_document(payload)


def _external_doc_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.update_document(payload)


def _external_task_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.list_tasks(payload)


def _external_task_create(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.create_task(payload)


def _external_task_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.external_connectors import ExternalConnectors

    return ExternalConnectors.update_task(payload)


def _oauth_token_list(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.core.oauth_token_store import OAuthTokenStore

    provider = str(payload.get("provider", "")).strip().lower()
    account_id = str(payload.get("account_id", "")).strip().lower()
    include_secrets = bool(payload.get("include_secrets", False))
    limit = max(1, min(_to_int(payload.get("limit", 200), 200), 2000))
    return OAuthTokenStore.shared().list(
        provider=provider,
        account_id=account_id,
        include_secrets=include_secrets,
        limit=limit,
    )


def _oauth_token_upsert(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.core.oauth_token_store import OAuthTokenStore

    provider = str(payload.get("provider", "")).strip().lower()
    account_id = str(payload.get("account_id", "default")).strip().lower() or "default"
    access_token = str(payload.get("access_token", "")).strip()
    if not provider:
        return {"status": "error", "message": "provider is required"}
    if not access_token:
        return {"status": "error", "message": "access_token is required"}

    refresh_token = str(payload.get("refresh_token", "")).strip()
    token_type = str(payload.get("token_type", "Bearer")).strip() or "Bearer"
    scopes = payload.get("scopes")
    expires_at = str(payload.get("expires_at", "")).strip()
    expires_in_s = payload.get("expires_in_s")
    parsed_expires_in: int | None = None
    if expires_in_s is not None and str(expires_in_s).strip():
        try:
            parsed_expires_in = int(expires_in_s)
        except Exception:
            return {"status": "error", "message": "expires_in_s must be an integer"}
    metadata = payload.get("metadata")
    return OAuthTokenStore.shared().upsert(
        provider=provider,
        account_id=account_id,
        access_token=access_token,
        refresh_token=refresh_token,
        token_type=token_type,
        scopes=scopes if isinstance(scopes, list) else None,
        expires_at=expires_at,
        expires_in_s=parsed_expires_in,
        metadata=metadata if isinstance(metadata, dict) else {},
    )


def _oauth_token_refresh(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.core.oauth_token_store import OAuthTokenStore

    provider = str(payload.get("provider", "")).strip().lower()
    if not provider:
        return {"status": "error", "message": "provider is required"}
    account_id = str(payload.get("account_id", "default")).strip().lower() or "default"
    return OAuthTokenStore.shared().refresh(provider=provider, account_id=account_id)


def _oauth_token_maintain(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.core.oauth_token_store import OAuthTokenStore

    provider = str(payload.get("provider", "")).strip().lower()
    account_id = str(payload.get("account_id", "")).strip().lower()
    refresh_window_s = _to_int(payload.get("refresh_window_s", 300), 300)
    refresh_window_s = max(0, min(refresh_window_s, 86400 * 7))
    dry_run = bool(payload.get("dry_run", False))
    return OAuthTokenStore.shared().maintain(
        refresh_window_s=refresh_window_s,
        provider=provider,
        account_id=account_id,
        dry_run=dry_run,
    )


def _oauth_token_revoke(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.core.oauth_token_store import OAuthTokenStore

    provider = str(payload.get("provider", "")).strip().lower()
    if not provider:
        return {"status": "error", "message": "provider is required"}
    account_id = str(payload.get("account_id", "default")).strip().lower() or "default"
    return OAuthTokenStore.shared().revoke(provider=provider, account_id=account_id)


def _accessibility_status(_: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.accessibility_tools import AccessibilityTools

    return AccessibilityTools.health()


def _accessibility_list_elements(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.accessibility_tools import AccessibilityTools

    window_title = str(payload.get("window_title", "")).strip()
    query = str(payload.get("query", "")).strip()
    control_type = str(payload.get("control_type", "")).strip()
    include_descendants = bool(payload.get("include_descendants", True))
    max_elements = max(1, min(_to_int(payload.get("max_elements", 150), 150), 1000))
    return AccessibilityTools.list_elements(
        window_title=window_title,
        query=query,
        control_type=control_type,
        include_descendants=include_descendants,
        max_elements=max_elements,
    )


def _accessibility_find_element(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.accessibility_tools import AccessibilityTools

    query = str(payload.get("query", "")).strip()
    if not query:
        return {"status": "error", "message": "query is required"}
    window_title = str(payload.get("window_title", "")).strip()
    control_type = str(payload.get("control_type", "")).strip()
    max_results = max(1, min(_to_int(payload.get("max_results", 10), 10), 100))
    return AccessibilityTools.find_element(
        query=query,
        window_title=window_title,
        control_type=control_type,
        max_results=max_results,
    )


def _accessibility_invoke_element(payload: Dict[str, Any]) -> Dict[str, Any]:
    from backend.python.tools.accessibility_tools import AccessibilityTools

    element_id = str(payload.get("element_id", "")).strip()
    query = str(payload.get("query", "")).strip()
    if not element_id and not query:
        return {"status": "error", "message": "element_id or query is required"}
    action = str(payload.get("action", "click")).strip().lower() or "click"
    window_title = str(payload.get("window_title", "")).strip()
    control_type = str(payload.get("control_type", "")).strip()
    click_offset_x = _to_int(payload.get("click_offset_x", 0), 0)
    click_offset_y = _to_int(payload.get("click_offset_y", 0), 0)
    return AccessibilityTools.invoke_element(
        element_id=element_id,
        query=query,
        action=action,
        window_title=window_title,
        control_type=control_type,
        click_offset_x=click_offset_x,
        click_offset_y=click_offset_y,
    )


@route("open_app")
def open_app_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _open_app(payload)


@route("open_url")
def open_url_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _open_url(payload)


@route("media_search")
def media_search_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _media_search(payload)


@route("defender_status")
def defender_status_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _defender_status(payload)


@route("system_snapshot")
def system_snapshot_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _system_snapshot(payload)


@route("list_processes")
def list_processes_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _list_processes(payload)


@route("terminate_process")
def terminate_process_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _terminate_process(payload)


@route("list_windows")
def list_windows_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _list_windows(payload)


@route("active_window")
def active_window_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _active_window(payload)


@route("focus_window")
def focus_window_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _focus_window(payload)


@route("focus_related_window")
def focus_related_window_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _focus_related_window(payload)


@route("focus_related_window_chain")
def focus_related_window_chain_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _focus_related_window_chain(payload)


@route("media_info")
async def media_info_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_info(payload)


@route("media_play_pause")
async def media_play_pause_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_play_pause(payload)


@route("media_play")
async def media_play_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_play(payload)


@route("media_pause")
async def media_pause_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_pause(payload)


@route("media_stop")
async def media_stop_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_stop(payload)


@route("media_next")
async def media_next_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_next(payload)


@route("media_previous")
async def media_previous_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _media_previous(payload)


@route("send_notification")
def send_notification_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _send_notification(payload)


@route("search_files")
def search_files_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _search_files(payload)


@route("search_text")
def search_text_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _search_text(payload)


@route("scan_directory")
def scan_directory_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _scan_directory(payload)


@route("hash_file")
def hash_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _hash_file(payload)


@route("backup_file")
def backup_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _backup_file(payload)


@route("copy_file")
def copy_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _copy_file(payload)


@route("list_folder")
def list_folder_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _list_folder(payload)


@route("create_folder")
def create_folder_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _create_folder(payload)


@route("folder_size")
def folder_size_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _folder_size(payload)


@route("explorer_open_path")
def explorer_open_path_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _explorer_open_path(payload)


@route("explorer_select_file")
def explorer_select_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _explorer_select_file(payload)


@route("list_files")
def list_files_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _list_files(payload)


@route("read_file")
def read_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _read_file(payload)


@route("write_file")
def write_file_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _write_file(payload)


@route("time_now")
def time_now_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _time_now(payload)


@route("clipboard_read")
def clipboard_read_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _clipboard_read(payload)


@route("clipboard_write")
def clipboard_write_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _clipboard_write(payload)


@route("keyboard_type")
def keyboard_type_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _keyboard_type(payload)


@route("keyboard_hotkey")
def keyboard_hotkey_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _keyboard_hotkey(payload)


@route("mouse_move")
def mouse_move_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _mouse_move(payload)


@route("mouse_click")
def mouse_click_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _mouse_click(payload)


@route("mouse_scroll")
def mouse_scroll_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _mouse_scroll(payload)


@route("screenshot_capture")
def screenshot_capture_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _screenshot_capture(payload)


@route("browser_read_dom")
def browser_read_dom_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_read_dom(payload)


@route("browser_extract_links")
def browser_extract_links_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_extract_links(payload)


@route("browser_session_create")
def browser_session_create_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_create(payload)


@route("browser_session_list")
def browser_session_list_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_list(payload)


@route("browser_session_close")
def browser_session_close_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_close(payload)


@route("browser_session_request")
def browser_session_request_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_request(payload)


@route("browser_session_read_dom")
def browser_session_read_dom_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_read_dom(payload)


@route("browser_session_extract_links")
def browser_session_extract_links_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _browser_session_extract_links(payload)


@route("computer_observe")
def computer_observe_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_observe(payload)


@route("computer_assert_text_visible")
def computer_assert_text_visible_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_assert_text_visible(payload)


@route("computer_find_text_targets")
def computer_find_text_targets_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_find_text_targets(payload)


@route("computer_wait_for_text")
def computer_wait_for_text_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_wait_for_text(payload)


@route("computer_click_text")
def computer_click_text_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_click_text(payload)


@route("computer_click_target")
def computer_click_target_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _computer_click_target(payload)


@route("extract_text_from_image")
def extract_text_from_image_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _extract_text_from_image(payload)


@route("run_whitelisted_app")
def run_whitelisted_app_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _run_whitelisted_app(payload)


@route("run_trusted_script")
def run_trusted_script_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _run_trusted_script(payload)


@route("tts_speak")
def tts_speak_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _tts_speak(payload)


@route("tts_stop")
def tts_stop_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _tts_stop(payload)


@route("tts_diagnostics")
def tts_diagnostics_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _tts_diagnostics(payload)


@route("tts_policy_status")
def tts_policy_status_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _tts_policy_status(payload)


@route("tts_policy_update")
def tts_policy_update_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _tts_policy_update(payload)


@route("external_connector_status")
def external_connector_status_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_connector_status(payload)


@route("external_connector_preflight")
def external_connector_preflight_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_connector_preflight(payload)


@route("external_email_send")
def external_email_send_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_email_send(payload)


@route("external_email_list")
def external_email_list_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_email_list(payload)


@route("external_email_read")
def external_email_read_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_email_read(payload)


@route("external_calendar_create_event")
def external_calendar_create_event_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_calendar_create_event(payload)


@route("external_calendar_list_events")
def external_calendar_list_events_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_calendar_list_events(payload)


@route("external_calendar_update_event")
def external_calendar_update_event_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_calendar_update_event(payload)


@route("external_doc_create")
def external_doc_create_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_doc_create(payload)


@route("external_doc_list")
def external_doc_list_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_doc_list(payload)


@route("external_doc_read")
def external_doc_read_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_doc_read(payload)


@route("external_doc_update")
def external_doc_update_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_doc_update(payload)


@route("external_task_list")
def external_task_list_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_task_list(payload)


@route("external_task_create")
def external_task_create_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_task_create(payload)


@route("external_task_update")
def external_task_update_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _external_task_update(payload)


@route("oauth_token_list")
def oauth_token_list_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _oauth_token_list(payload)


@route("oauth_token_upsert")
def oauth_token_upsert_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _oauth_token_upsert(payload)


@route("oauth_token_refresh")
def oauth_token_refresh_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _oauth_token_refresh(payload)


@route("oauth_token_maintain")
def oauth_token_maintain_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _oauth_token_maintain(payload)


@route("oauth_token_revoke")
def oauth_token_revoke_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _oauth_token_revoke(payload)


@route("accessibility_status")
def accessibility_status_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _accessibility_status(payload)


@route("accessibility_list_elements")
def accessibility_list_elements_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _accessibility_list_elements(payload)


@route("accessibility_find_element")
def accessibility_find_element_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _accessibility_find_element(payload)


@route("accessibility_invoke_element")
def accessibility_invoke_element_route(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _accessibility_invoke_element(payload)


def register_tools(registry: ToolRegistry) -> None:
    registry.register("open_app", _open_app, description="Launch a desktop application by executable or registered name.", risk="medium")
    registry.register("open_url", _open_url, description="Open a URL in the default browser.", risk="low", required_args=["url"])
    registry.register("media_search", _media_search, description="Search media content in browser.", risk="low", required_args=["query"])
    registry.register("defender_status", _defender_status, description="Read Microsoft Defender security status.", risk="low")
    registry.register("system_snapshot", _system_snapshot, description="Capture CPU/RAM/disk/network/battery/system metrics snapshot.", risk="low")
    registry.register("list_processes", _list_processes, description="List active system processes by CPU usage.", risk="low")
    registry.register("terminate_process", _terminate_process, description="Terminate a process by pid or name.", risk="high", requires_confirmation=True)
    registry.register("list_windows", _list_windows, description="List visible top-level windows.", risk="low")
    registry.register("active_window", _active_window, description="Get active foreground window details.", risk="low")
    registry.register("focus_window", _focus_window, description="Focus a window by title substring or hwnd.", risk="medium")
    registry.register(
        "focus_related_window",
        _focus_related_window,
        description="Focus the best related child or descendant window using native topology hints.",
        risk="medium",
    )
    registry.register(
        "focus_related_window_chain",
        _focus_related_window_chain,
        description="Follow a related child-window chain through bounded native descendant adoption.",
        risk="medium",
    )
    registry.register("media_info", _media_info, description="Get current media playback metadata.", risk="low")
    registry.register("media_play_pause", _media_play_pause, description="Toggle play/pause for active media session.", risk="medium")
    registry.register("media_play", _media_play, description="Play active media session.", risk="medium")
    registry.register("media_pause", _media_pause, description="Pause active media session.", risk="medium")
    registry.register("media_stop", _media_stop, description="Stop active media session.", risk="medium")
    registry.register("media_next", _media_next, description="Skip to next media track.", risk="medium")
    registry.register("media_previous", _media_previous, description="Go to previous media track.", risk="medium")
    registry.register("send_notification", _send_notification, description="Send a local desktop notification.", risk="low", required_args=["message"])
    registry.register("search_files", _search_files, description="Search files by glob pattern inside allowed roots.", risk="medium")
    registry.register("search_text", _search_text, description="Search text inside files in allowed roots.", risk="medium", required_args=["keyword"])
    registry.register("scan_directory", _scan_directory, description="Recursively list files in a directory (allowed roots).", risk="medium")
    registry.register("hash_file", _hash_file, description="Compute file hash (sha256 by default).", risk="medium", required_args=["path"])
    registry.register("backup_file", _backup_file, description="Backup a file to a safe backup directory.", risk="medium", required_args=["source"])
    registry.register("copy_file", _copy_file, description="Copy a file to another path in allowed roots.", risk="high", requires_confirmation=True, required_args=["source", "destination"])
    registry.register("list_folder", _list_folder, description="List files and directories in a folder.", risk="low")
    registry.register("create_folder", _create_folder, description="Create a folder path recursively.", risk="medium", required_args=["path"])
    registry.register("folder_size", _folder_size, description="Get total size of a folder.", risk="low")
    registry.register("explorer_open_path", _explorer_open_path, description="Open a folder path in Explorer/file manager.", risk="medium")
    registry.register("explorer_select_file", _explorer_select_file, description="Reveal and select a file in Explorer/file manager.", risk="medium", required_args=["path"])
    registry.register("list_files", _list_files, description="List direct children of a directory.", risk="low")
    registry.register("read_file", _read_file, description="Read a text file inside allowed roots.", risk="medium", required_args=["path"])
    registry.register("write_file", _write_file, description="Write content into a text file inside allowed roots.", risk="high", requires_confirmation=True, required_args=["path", "content"])
    registry.register("time_now", _time_now, description="Get current time in a specified timezone.", risk="low")
    registry.register("clipboard_read", _clipboard_read, description="Read current clipboard text.", risk="medium")
    registry.register("clipboard_write", _clipboard_write, description="Write text into system clipboard.", risk="high", requires_confirmation=True, required_args=["text"])
    registry.register("keyboard_type", _keyboard_type, description="Type text into active window.", risk="high", requires_confirmation=True, required_args=["text"])
    registry.register("keyboard_hotkey", _keyboard_hotkey, description="Send a keyboard key or hotkey sequence.", risk="high", requires_confirmation=True)
    registry.register("mouse_move", _mouse_move, description="Move mouse cursor to absolute coordinates.", risk="high", requires_confirmation=True, required_args=["x", "y"])
    registry.register("mouse_click", _mouse_click, description="Click mouse at current or given coordinates.", risk="high", requires_confirmation=True)
    registry.register("mouse_scroll", _mouse_scroll, description="Scroll mouse wheel by amount.", risk="high", requires_confirmation=True)
    registry.register("screenshot_capture", _screenshot_capture, description="Capture screen image to file path.", risk="medium")
    registry.register("browser_read_dom", _browser_read_dom, description="Fetch webpage DOM text/title with network safety constraints.", risk="medium", required_args=["url"])
    registry.register("browser_extract_links", _browser_extract_links, description="Extract links from webpage DOM with optional same-domain filter.", risk="medium", required_args=["url"])
    registry.register("browser_session_create", _browser_session_create, description="Create authenticated browser session with cookies/headers/OAuth linkage.", risk="medium")
    registry.register("browser_session_list", _browser_session_list, description="List active browser sessions and metadata.", risk="low")
    registry.register("browser_session_close", _browser_session_close, description="Close and delete browser session.", risk="medium", required_args=["session_id"])
    registry.register("browser_session_request", _browser_session_request, description="Run authenticated HTTP request using browser session state.", risk="medium", required_args=["session_id", "url"])
    registry.register("browser_session_read_dom", _browser_session_read_dom, description="Read DOM text/title through authenticated browser session.", risk="medium", required_args=["session_id", "url"])
    registry.register("browser_session_extract_links", _browser_session_extract_links, description="Extract links through authenticated browser session.", risk="medium", required_args=["session_id", "url"])
    registry.register("computer_observe", _computer_observe, description="Capture screen and try OCR for current visual context.", risk="medium")
    registry.register("computer_assert_text_visible", _computer_assert_text_visible, description="Check whether specific text is visible on the current screen capture.", risk="high", requires_confirmation=True, required_args=["text"])
    registry.register("computer_find_text_targets", _computer_find_text_targets, description="Find OCR text targets with coordinates and confidence for UI grounding.", risk="medium", required_args=["query"])
    registry.register("computer_wait_for_text", _computer_wait_for_text, description="Wait until text appears/disappears on screen with timeout controls.", risk="medium", required_args=["text"])
    registry.register("computer_click_text", _computer_click_text, description="Locate text on screen and click its coordinates with post-action verification.", risk="high", requires_confirmation=True, required_args=["query"])
    registry.register("computer_click_target", _computer_click_target, description="Resolve and click UI targets with accessibility-first routing and OCR fallback verification.", risk="high", requires_confirmation=True, required_args=["query"])
    registry.register("extract_text_from_image", _extract_text_from_image, description="Extract OCR text from an image file.", risk="medium", required_args=["path"])
    registry.register("run_whitelisted_app", _run_whitelisted_app, description="Run app from strict automation whitelist.", risk="high", requires_confirmation=True, required_args=["app_name"])
    registry.register("run_trusted_script", _run_trusted_script, description="Run script from trusted_scripts directory.", risk="high", requires_confirmation=True, required_args=["script_name"])
    registry.register("tts_speak", _tts_speak, description="Speak text using cloud TTS or local text fallback.", risk="low", required_args=["text"])
    registry.register("tts_stop", _tts_stop, description="Interrupt/stop current TTS playback for barge-in and cancellation flows.", risk="low")
    registry.register("tts_diagnostics", _tts_diagnostics, description="Return runtime diagnostics for local/cloud TTS providers.", risk="low")
    registry.register("tts_policy_status", _tts_policy_status, description="Return adaptive TTS provider routing policy state and recommendations.", risk="low")
    registry.register("tts_policy_update", _tts_policy_update, description="Update adaptive TTS provider routing policy thresholds, weights, and persistence.", risk="medium")
    registry.register("external_connector_status", _external_connector_status, description="Report availability of configured cloud connector credentials.", risk="low")
    registry.register("external_connector_preflight", _external_connector_preflight, description="Run action-specific connector preflight contract diagnostics with remediation hints.", risk="low", required_args=["action"])
    registry.register("external_email_send", _external_email_send, description="Send email via Gmail API, Microsoft Graph, or SMTP connector.", risk="high", requires_confirmation=True, required_args=["to"])
    registry.register("external_email_list", _external_email_list, description="List email messages from Gmail or Microsoft Graph mailbox.", risk="medium")
    registry.register("external_email_read", _external_email_read, description="Read a specific email message by message_id.", risk="medium", required_args=["message_id"])
    registry.register("external_calendar_create_event", _external_calendar_create_event, description="Create calendar event via Google Calendar or Microsoft Graph.", risk="medium", required_args=["title"])
    registry.register("external_calendar_list_events", _external_calendar_list_events, description="List calendar events across Google Calendar or Microsoft Graph.", risk="medium")
    registry.register("external_calendar_update_event", _external_calendar_update_event, description="Update calendar event metadata by event_id.", risk="high", requires_confirmation=True, required_args=["event_id"])
    registry.register("external_doc_create", _external_doc_create, description="Create cloud document via Google Docs or Microsoft Graph Drive.", risk="medium", required_args=["title"])
    registry.register("external_doc_list", _external_doc_list, description="List cloud documents from Google Docs or Microsoft Graph Drive.", risk="medium")
    registry.register("external_doc_read", _external_doc_read, description="Read cloud document content by document_id.", risk="medium", required_args=["document_id"])
    registry.register("external_doc_update", _external_doc_update, description="Update cloud document title/content by document_id.", risk="high", requires_confirmation=True, required_args=["document_id"])
    registry.register("external_task_list", _external_task_list, description="List task items via Google Tasks or Microsoft To Do connectors.", risk="medium")
    registry.register("external_task_create", _external_task_create, description="Create task item via Google Tasks or Microsoft To Do connectors.", risk="medium", required_args=["title"])
    registry.register("external_task_update", _external_task_update, description="Update task fields/status via Google Tasks or Microsoft To Do connectors.", risk="high", requires_confirmation=True, required_args=["task_id"])
    registry.register("oauth_token_list", _oauth_token_list, description="List stored OAuth tokens with metadata and expiry diagnostics.", risk="low")
    registry.register("oauth_token_upsert", _oauth_token_upsert, description="Store or rotate OAuth tokens with lifecycle metadata.", risk="high", requires_confirmation=True, required_args=["provider", "access_token"])
    registry.register("oauth_token_refresh", _oauth_token_refresh, description="Refresh OAuth access token using stored refresh token.", risk="medium", required_args=["provider"])
    registry.register("oauth_token_maintain", _oauth_token_maintain, description="Proactively refresh near-expiry OAuth access tokens across accounts.", risk="medium")
    registry.register("oauth_token_revoke", _oauth_token_revoke, description="Revoke stored OAuth token record from local vault.", risk="high", requires_confirmation=True, required_args=["provider"])
    registry.register("accessibility_status", _accessibility_status, description="Report accessibility automation backend status and capabilities.", risk="low")
    registry.register("accessibility_list_elements", _accessibility_list_elements, description="Enumerate UI Automation elements with metadata and bounds.", risk="medium")
    registry.register("accessibility_find_element", _accessibility_find_element, description="Find UI elements by text and control type via accessibility tree.", risk="medium", required_args=["query"])
    registry.register("accessibility_invoke_element", _accessibility_invoke_element, description="Invoke/click focused accessibility element by ID or query.", risk="high", requires_confirmation=True)
