from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import psutil

try:
    from backend.python.native.windows import get_native_window_runtime
except Exception:  # noqa: BLE001
    get_native_window_runtime = None

try:
    import win32gui
    import win32con
    import win32process
except Exception:  # noqa: BLE001
    win32gui = None
    win32con = None
    win32process = None


class WindowManager:
    """Safe monitor-only window manager with lightweight surface heuristics."""

    _BROWSER_PROCESSES = {"chrome", "msedge", "firefox", "brave", "opera", "iexplore"}
    _EDITOR_PROCESSES = {"code", "cursor", "devenv", "notepad++", "sublime_text", "idea64", "pycharm64"}
    _TERMINAL_PROCESSES = {
        "windowsterminal",
        "windows terminal",
        "wt",
        "cmd",
        "powershell",
        "pwsh",
        "conhost",
        "alacritty",
    }
    _FILE_MANAGER_PROCESSES = {"explorer", "totalcmd64", "doublecmd"}
    _ADMIN_PROCESSES = {"mmc", "taskmgr", "regedit", "services", "devmgmt"}

    def __init__(self, *, native_runtime: Any | None = None) -> None:
        if native_runtime is not None:
            self._native_runtime = native_runtime
        elif callable(get_native_window_runtime):
            try:
                self._native_runtime = get_native_window_runtime()
            except Exception:  # noqa: BLE001
                self._native_runtime = None
        else:
            self._native_runtime = None

    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        text = str(value or "").strip().lower()
        return " ".join(text.split())

    @classmethod
    def _tokenize(cls, value: Any) -> List[str]:
        text = cls._normalize_text(value)
        if not text:
            return []
        normalized = text
        for token in ("|", "-", "_", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ",", ":"):
            normalized = normalized.replace(token, " ")
        return [part for part in normalized.split() if part]

    @classmethod
    def _derive_app_name(cls, *, exe: str = "", process_name: str = "", title: str = "") -> str:
        base = Path(str(exe or "")).stem.strip().lower()
        if base:
            return base
        process_clean = cls._normalize_text(process_name).replace(".exe", "").strip()
        if process_clean:
            return process_clean
        tokens = cls._tokenize(title)
        return tokens[0] if tokens else "unknown"

    @classmethod
    def _build_window_signature(
        cls,
        *,
        title: str = "",
        exe: str = "",
        process_name: str = "",
        class_name: str = "",
        rect: tuple[int, int, int, int] | None = None,
    ) -> str:
        app_name = cls._derive_app_name(exe=exe, process_name=process_name, title=title)
        title_tokens = cls._tokenize(title)[:6]
        class_token = cls._normalize_text(class_name).replace(" ", "_")
        if rect:
            left, top, right, bottom = rect
            width = max(0, int(right) - int(left))
            height = max(0, int(bottom) - int(top))
            size_token = f"{width}x{height}"
        else:
            size_token = "0x0"
        title_fragment = "_".join(title_tokens) or "untitled"
        return f"{app_name}|{class_token or 'window'}|{size_token}|{title_fragment}"

    @classmethod
    def _infer_surface_hints(
        cls,
        *,
        title: str = "",
        process_name: str = "",
        class_name: str = "",
        app_name: str = "",
    ) -> Dict[str, bool]:
        title_norm = cls._normalize_text(title)
        process_norm = cls._normalize_text(process_name).replace(".exe", "")
        class_norm = cls._normalize_text(class_name)
        app_norm = cls._normalize_text(app_name)

        combined = " ".join(
            part for part in {title_norm, process_norm, class_norm, app_norm} if part
        )
        dialog_like = (
            "#32770" in class_name
            or "dialog" in combined
            or any(token in combined for token in ("properties", "options", "warning", "error", "confirm", "permission"))
        )
        browser_like = process_norm in cls._BROWSER_PROCESSES or app_norm in cls._BROWSER_PROCESSES
        editor_like = process_norm in cls._EDITOR_PROCESSES or app_norm in cls._EDITOR_PROCESSES
        terminal_like = process_norm in cls._TERMINAL_PROCESSES or app_norm in cls._TERMINAL_PROCESSES
        file_manager_like = process_norm in cls._FILE_MANAGER_PROCESSES or app_norm in cls._FILE_MANAGER_PROCESSES
        settings_like = "settings" in combined or "control panel" in combined
        admin_like = (
            process_norm in cls._ADMIN_PROCESSES
            or any(token in combined for token in ("device manager", "event viewer", "task scheduler", "registry editor", "services"))
        )
        return {
            "dialog_like": bool(dialog_like),
            "browser_like": bool(browser_like),
            "editor_like": bool(editor_like),
            "terminal_like": bool(terminal_like),
            "file_manager_like": bool(file_manager_like),
            "settings_like": bool(settings_like),
            "admin_like": bool(admin_like),
        }

    @classmethod
    def _text_match_score(cls, haystack: Any, needle: Any) -> float:
        clean_haystack = cls._normalize_text(haystack)
        clean_needle = cls._normalize_text(needle)
        if not clean_haystack or not clean_needle:
            return 0.0
        if clean_haystack == clean_needle:
            return 1.0
        if clean_needle in clean_haystack:
            coverage = len(clean_needle) / max(1, len(clean_haystack))
            return round(max(0.38, min(0.96, coverage + 0.18)), 4)
        haystack_tokens = set(cls._tokenize(clean_haystack))
        needle_tokens = set(cls._tokenize(clean_needle))
        if not haystack_tokens or not needle_tokens:
            return 0.0
        overlap = len(haystack_tokens & needle_tokens)
        if overlap <= 0:
            return 0.0
        return round(overlap / max(1, len(needle_tokens)), 4)

    def _compose_window_info(self, raw: Dict[str, Any], *, observation_backend: str) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            return {}

        title = str(raw.get("title", "") or "")
        exe = str(raw.get("exe", "") or "")
        process_name = str(raw.get("process_name", "") or "")
        if not process_name and exe:
            process_name = Path(exe).name
        class_name = str(raw.get("class_name", "") or "")
        hwnd = int(raw.get("hwnd", 0) or 0)
        owner_hwnd = int(raw.get("owner_hwnd", 0) or 0)
        root_owner_hwnd = int(raw.get("root_owner_hwnd", 0) or 0)
        owner_chain_depth = max(0, int(raw.get("owner_chain_depth", 0) or 0))
        pid = int(raw.get("pid", 0) or 0)
        left = int(raw.get("left", 0) or 0)
        top = int(raw.get("top", 0) or 0)
        right = int(raw.get("right", 0) or 0)
        bottom = int(raw.get("bottom", 0) or 0)
        width = max(0, right - left)
        height = max(0, bottom - top)
        app_name = self._derive_app_name(exe=exe, process_name=process_name, title=title)
        surface_hints = self._infer_surface_hints(
            title=title,
            process_name=process_name,
            class_name=class_name,
            app_name=app_name,
        )
        signature = self._build_window_signature(
            title=title,
            exe=exe,
            process_name=process_name,
            class_name=class_name,
            rect=(left, top, right, bottom),
        )
        return {
            "hwnd": hwnd,
            "owner_hwnd": owner_hwnd,
            "root_owner_hwnd": root_owner_hwnd,
            "owner_chain_depth": owner_chain_depth,
            "title": title,
            "pid": pid,
            "exe": exe,
            "process_name": process_name,
            "app_name": app_name,
            "class_name": class_name,
            "visible": bool(raw.get("visible", False)),
            "enabled": bool(raw.get("enabled", False)),
            "minimized": bool(raw.get("minimized", False)),
            "maximized": bool(raw.get("maximized", False)),
            "is_foreground": bool(raw.get("is_foreground", False)),
            "width": width,
            "height": height,
            "area": width * height,
            "title_tokens": self._tokenize(title),
            "window_signature": signature,
            "surface_hints": surface_hints,
            "position": {
                "left": left,
                "top": top,
                "right": right,
                "bottom": bottom,
            },
            "observation_backend": observation_backend,
        }

    @staticmethod
    def _enrich_owner_chain_metrics(windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in windows if isinstance(row, dict)]
        if not rows:
            return []
        hwnd_map = {
            int(row.get("hwnd", 0) or 0): row
            for row in rows
            if int(row.get("hwnd", 0) or 0) > 0
        }
        cache: Dict[int, tuple[int, int]] = {}

        def _resolve(hwnd: int) -> tuple[int, int]:
            clean_hwnd = int(hwnd or 0)
            if clean_hwnd <= 0:
                return (0, 0)
            if clean_hwnd in cache:
                return cache[clean_hwnd]
            row = hwnd_map.get(clean_hwnd, {})
            owner_hwnd = int(row.get("owner_hwnd", 0) or 0) if isinstance(row, dict) else 0
            explicit_root = int(row.get("root_owner_hwnd", 0) or 0) if isinstance(row, dict) else 0
            explicit_depth = max(0, int(row.get("owner_chain_depth", 0) or 0)) if isinstance(row, dict) else 0
            if owner_hwnd <= 0:
                resolved = (explicit_root or clean_hwnd, explicit_depth if explicit_root else 0)
                cache[clean_hwnd] = resolved
                return resolved
            seen = {clean_hwnd}
            current_owner = owner_hwnd
            depth = 0
            root_owner = explicit_root or clean_hwnd
            while current_owner > 0 and current_owner not in seen and depth < 24:
                seen.add(current_owner)
                depth += 1
                root_owner = current_owner
                next_row = hwnd_map.get(current_owner, {})
                next_owner = int(next_row.get("owner_hwnd", 0) or 0) if isinstance(next_row, dict) else 0
                if next_owner <= 0:
                    break
                current_owner = next_owner
            resolved = (explicit_root or root_owner or clean_hwnd, max(explicit_depth, depth))
            cache[clean_hwnd] = resolved
            return resolved

        enriched: List[Dict[str, Any]] = []
        for row in rows:
            row_payload = dict(row)
            hwnd = int(row_payload.get("hwnd", 0) or 0)
            root_owner_hwnd, owner_chain_depth = _resolve(hwnd)
            if hwnd > 0:
                row_payload["root_owner_hwnd"] = root_owner_hwnd or hwnd
            else:
                row_payload["root_owner_hwnd"] = 0
            row_payload["owner_chain_depth"] = owner_chain_depth
            enriched.append(row_payload)
        return enriched

    @staticmethod
    def _merge_owner_metrics(window: Dict[str, Any], *, windows: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = dict(window) if isinstance(window, dict) else {}
        if not payload:
            return {}
        hwnd = int(payload.get("hwnd", 0) or 0)
        if hwnd <= 0:
            return payload
        match = next(
            (
                dict(row)
                for row in windows
                if isinstance(row, dict) and int(row.get("hwnd", 0) or 0) == hwnd
            ),
            {},
        )
        if match:
            payload["root_owner_hwnd"] = int(match.get("root_owner_hwnd", payload.get("root_owner_hwnd", hwnd)) or hwnd)
            payload["owner_chain_depth"] = max(0, int(match.get("owner_chain_depth", payload.get("owner_chain_depth", 0)) or 0))
        else:
            payload["root_owner_hwnd"] = int(payload.get("root_owner_hwnd", hwnd) or hwnd)
            payload["owner_chain_depth"] = max(0, int(payload.get("owner_chain_depth", 0) or 0))
        return payload

    @staticmethod
    def _owner_chain_rows(window: Dict[str, Any], *, windows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        payload = dict(window) if isinstance(window, dict) else {}
        if not payload:
            return []
        hwnd_map = {
            int(row.get("hwnd", 0) or 0): dict(row)
            for row in windows
            if isinstance(row, dict) and int(row.get("hwnd", 0) or 0) > 0
        }
        current_hwnd = int(payload.get("hwnd", 0) or 0)
        current_row = hwnd_map.get(current_hwnd, payload)
        chain: List[Dict[str, Any]] = []
        seen: set[int] = set()
        while isinstance(current_row, dict):
            chain.append(dict(current_row))
            row_hwnd = int(current_row.get("hwnd", 0) or 0)
            if row_hwnd <= 0 or row_hwnd in seen:
                break
            seen.add(row_hwnd)
            owner_hwnd = int(current_row.get("owner_hwnd", 0) or 0)
            if owner_hwnd <= 0:
                break
            current_row = hwnd_map.get(owner_hwnd, {})
            if not current_row:
                break
        chain.reverse()
        return chain[:8]

    def _window_relation_score(
        self,
        *,
        window: Dict[str, Any],
        query: str = "",
        app_name: str = "",
        window_title: str = "",
        window_signature: str = "",
        hwnd: int = 0,
        owner_hwnd: int = 0,
        root_owner_hwnd: int = 0,
        owner_chain_depth: int = 0,
        pid: int = 0,
        parent_hwnd: int = 0,
        parent_pid: int = 0,
    ) -> Dict[str, Any]:
        if not isinstance(window, dict):
            return {"score": 0.0, "reasons": []}
        score = 0.0
        reasons: List[str] = []
        candidate_hwnd = int(window.get("hwnd", 0) or 0)
        candidate_owner_hwnd = int(window.get("owner_hwnd", 0) or 0)
        candidate_root_owner_hwnd = int(window.get("root_owner_hwnd", candidate_hwnd or 0) or candidate_hwnd or 0)
        candidate_owner_chain_depth = max(0, int(window.get("owner_chain_depth", 0) or 0))
        candidate_pid = int(window.get("pid", 0) or 0)
        candidate_title = str(window.get("title", "") or "").strip()
        candidate_process = str(window.get("process_name", "") or "").strip()
        candidate_app_name = str(window.get("app_name", "") or "").strip()
        candidate_signature = str(window.get("window_signature", "") or "").strip()
        if hwnd and candidate_hwnd and candidate_hwnd == int(hwnd):
            score += 2.4
            reasons.append("exact_hwnd")
        if pid and candidate_pid and candidate_pid == int(pid):
            score += 1.4
            reasons.append("same_pid")
        if parent_hwnd and candidate_hwnd and candidate_hwnd == int(parent_hwnd):
            score += 0.55
            reasons.append("same_parent_hwnd")
        if owner_hwnd and candidate_hwnd and candidate_hwnd == int(owner_hwnd):
            score += 0.7
            reasons.append("same_owner_hwnd")
        if root_owner_hwnd and candidate_root_owner_hwnd and candidate_root_owner_hwnd == int(root_owner_hwnd):
            score += 0.68
            reasons.append("same_root_owner_hwnd")
        if hwnd and candidate_owner_hwnd and candidate_owner_hwnd == int(hwnd):
            score += 1.05
            reasons.append("owned_by_hwnd")
        elif owner_hwnd and candidate_owner_hwnd and candidate_owner_hwnd == int(owner_hwnd):
            score += 0.95
            reasons.append("owned_by_owner_hwnd")
        elif parent_hwnd and candidate_owner_hwnd and candidate_owner_hwnd == int(parent_hwnd):
            score += 0.82
            reasons.append("owned_by_parent_hwnd")
        if parent_pid and candidate_pid and candidate_pid == int(parent_pid):
            score += 0.7
            reasons.append("same_parent_pid")

        signature_score = self._text_match_score(candidate_signature, window_signature)
        if signature_score > 0:
            score += 0.7 * signature_score
            reasons.append("signature")

        title_score = self._text_match_score(candidate_title, window_title)
        if title_score > 0:
            score += 0.95 * title_score
            reasons.append("window_title")

        app_title_score = self._text_match_score(candidate_title, app_name)
        app_process_score = max(
            self._text_match_score(candidate_process, app_name),
            self._text_match_score(candidate_app_name, app_name),
        )
        if app_title_score > 0:
            score += 0.42 * app_title_score
            reasons.append("app_title")
        if app_process_score > 0:
            score += 0.78 * app_process_score
            reasons.append("app_process")

        query_score = max(
            self._text_match_score(candidate_title, query),
            self._text_match_score(candidate_process, query),
            self._text_match_score(candidate_signature, query),
        )
        if query_score > 0:
            score += 0.36 * query_score
            reasons.append("query")
        if query_score >= 0.95:
            if hwnd and candidate_owner_hwnd and candidate_owner_hwnd == int(hwnd):
                score += 1.25
                reasons.append("query_owned_child")
            elif root_owner_hwnd and candidate_root_owner_hwnd and candidate_root_owner_hwnd == int(root_owner_hwnd):
                score += 0.72
                reasons.append("query_same_root_owner")
            elif parent_hwnd and candidate_owner_hwnd and candidate_owner_hwnd == int(parent_hwnd):
                score += 1.05
                reasons.append("query_parent_owned_child")
            elif pid and candidate_pid and candidate_pid == int(pid):
                score += 0.55
                reasons.append("query_same_pid_exact")
        if candidate_owner_chain_depth > owner_chain_depth and candidate_root_owner_hwnd and root_owner_hwnd and candidate_root_owner_hwnd == int(root_owner_hwnd):
            score += min(0.22, 0.08 * max(1, candidate_owner_chain_depth - owner_chain_depth))
            reasons.append("deeper_owner_chain")

        if bool(window.get("is_foreground", False)):
            score += 0.08
            reasons.append("foreground")
        if bool(window.get("visible", False)) and bool(window.get("enabled", False)):
            score += 0.05
        if bool(window.get("surface_hints", {}).get("dialog_like", False)) if isinstance(window.get("surface_hints", {}), dict) else False:
            if query_score > 0 or title_score > 0 or parent_pid:
                score += 0.05
                reasons.append("dialog_related")
        return {
            "score": round(score, 4),
            "reasons": reasons,
        }

    def _related_window_cluster(
        self,
        *,
        windows: List[Dict[str, Any]],
        seed_window: Dict[str, Any],
        app_name: str = "",
        window_title: str = "",
        query: str = "",
    ) -> List[Dict[str, Any]]:
        seed = dict(seed_window) if isinstance(seed_window, dict) else {}
        seed_pid = int(seed.get("pid", 0) or 0)
        seed_hwnd = int(seed.get("hwnd", 0) or 0)
        seed_owner_hwnd = int(seed.get("owner_hwnd", 0) or 0)
        seed_root_owner_hwnd = int(seed.get("root_owner_hwnd", seed_hwnd or 0) or seed_hwnd or 0)
        seed_owner_chain_depth = max(0, int(seed.get("owner_chain_depth", 0) or 0))
        scored_rows: List[tuple[float, Dict[str, Any]]] = []
        for row in windows:
            relation = self._window_relation_score(
                window=row,
                query=query,
                app_name=app_name or str(seed.get("app_name", "") or "").strip(),
                window_title=window_title or str(seed.get("title", "") or "").strip(),
                window_signature=str(seed.get("window_signature", "") or "").strip(),
                owner_hwnd=seed_owner_hwnd,
                root_owner_hwnd=seed_root_owner_hwnd,
                owner_chain_depth=seed_owner_chain_depth,
                pid=seed_pid,
                hwnd=seed_hwnd,
                parent_pid=seed_pid,
                parent_hwnd=seed_hwnd,
            )
            relation_score = float(relation.get("score", 0.0) or 0.0)
            if relation_score <= 0:
                continue
            enriched = dict(row)
            enriched["relation_score"] = round(relation_score, 4)
            enriched["relation_reasons"] = list(relation.get("reasons", []))
            scored_rows.append((relation_score, enriched))
        scored_rows.sort(
            key=lambda item: (
                -item[0],
                -int(item[1].get("area", 0) or 0),
                str(item[1].get("title", "") or "").lower(),
            )
        )
        return [row for _score, row in scored_rows[:8]]

    def _native_list_windows(self, *, limit: int = 300) -> List[Dict[str, Any]] | None:
        if self._native_runtime is None:
            return None
        try:
            payload = self._native_runtime.list_windows(limit=limit)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(payload, dict) or payload.get("status") != "success":
            return None
        backend = str(payload.get("backend", "cpp_cython") or "cpp_cython")
        windows: List[Dict[str, Any]] = []
        for item in payload.get("windows", []) or []:
            normalized = self._compose_window_info(item, observation_backend=backend)
            if normalized:
                windows.append(normalized)
        return self._enrich_owner_chain_metrics(windows)

    def _native_active_window(self) -> Dict[str, Any] | None:
        if self._native_runtime is None:
            return None
        try:
            payload = self._native_runtime.active_window()
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(payload, dict) or payload.get("status") != "success":
            return None
        backend = str(payload.get("backend", "cpp_cython") or "cpp_cython")
        window = self._compose_window_info(payload.get("window", {}), observation_backend=backend)
        if not window:
            return None
        return self._merge_owner_metrics(window, windows=self.list_windows())

    def _native_focus_window(self, *, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any] | None:
        if self._native_runtime is None:
            return None
        try:
            payload = self._native_runtime.focus_window(title_contains=title_contains, hwnd=hwnd)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(payload, dict) or payload.get("status") != "success":
            return None
        backend = str(payload.get("backend", "cpp_cython") or "cpp_cython")
        window = self._compose_window_info(payload.get("window", {}), observation_backend=backend)
        if not window:
            return None
        return {
            "status": "success",
            "focus_applied": bool(payload.get("focus_applied", False)),
            "window": window,
        }

    def _native_reacquire_window(
        self,
        *,
        query: str = "",
        window_title: str = "",
        hwnd: int | None = None,
        pid: int | None = None,
        limit: int = 80,
    ) -> Dict[str, Any] | None:
        if self._native_runtime is None:
            return None
        try:
            payload = self._native_runtime.reacquire_related_window(
                query=query,
                window_title=window_title,
                hwnd=hwnd,
                pid=pid,
                limit=limit,
            )
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(payload, dict) or payload.get("status") != "success":
            return None
        backend = str(payload.get("backend", "cpp_cython") or "cpp_cython")
        candidate = self._compose_window_info(payload.get("candidate", {}), observation_backend=backend)
        if not candidate:
            return None
        candidates = [
            normalized
            for normalized in (
                self._compose_window_info(item, observation_backend=backend)
                for item in payload.get("candidates", []) or []
            )
            if normalized
        ]
        candidates = self._enrich_owner_chain_metrics(candidates)
        if not any(int(row.get("hwnd", 0) or 0) == int(candidate.get("hwnd", 0) or 0) for row in candidates):
            candidates.insert(0, candidate)
        candidates = self._enrich_owner_chain_metrics(candidates)
        related_windows = self._related_window_cluster(
            windows=candidates,
            seed_window=candidate,
            query=query,
            window_title=window_title,
            app_name=str(candidate.get("app_name", "") or "").strip(),
        )
        candidate_hwnd = int(candidate.get("hwnd", 0) or 0)
        candidate_root_owner_hwnd = int(candidate.get("root_owner_hwnd", 0) or candidate_hwnd or 0)
        same_root_owner_windows = [
            dict(row)
            for row in candidates
            if candidate_root_owner_hwnd and int(row.get("root_owner_hwnd", 0) or 0) == candidate_root_owner_hwnd
        ]
        same_root_owner_dialog_windows = [
            dict(row)
            for row in same_root_owner_windows
            if isinstance(row.get("surface_hints", {}), dict)
            and bool(row.get("surface_hints", {}).get("dialog_like", False))
        ]
        owner_linked_windows = [
            dict(row)
            for row in related_windows
            if (
                candidate_hwnd and int(row.get("owner_hwnd", 0) or 0) == candidate_hwnd
            )
            or (
                candidate_root_owner_hwnd
                and int(row.get("root_owner_hwnd", 0) or 0) == candidate_root_owner_hwnd
            )
        ]
        owner_chain_titles = [
            str(item).strip()
            for item in payload.get("owner_chain_titles", [])
            if str(item).strip()
        ] if isinstance(payload.get("owner_chain_titles", []), list) else [
            str(row.get("title", "") or "").strip()
            for row in self._owner_chain_rows(candidate, windows=candidates)
            if str(row.get("title", "") or "").strip()
        ]
        same_root_owner_titles = [
            str(item).strip()
            for item in payload.get("same_root_owner_titles", [])
            if str(item).strip()
        ] if isinstance(payload.get("same_root_owner_titles", []), list) else [
            str(row.get("title", "") or "").strip()
            for row in same_root_owner_windows[:6]
            if str(row.get("title", "") or "").strip()
        ]
        same_root_owner_dialog_titles = [
            str(item).strip()
            for item in payload.get("same_root_owner_dialog_titles", [])
            if str(item).strip()
        ] if isinstance(payload.get("same_root_owner_dialog_titles", []), list) else [
            str(row.get("title", "") or "").strip()
            for row in same_root_owner_dialog_windows[:6]
            if str(row.get("title", "") or "").strip()
        ]
        modal_chain_signature = "|".join(
            [
                str(candidate_root_owner_hwnd or 0),
                str(len(same_root_owner_dialog_windows)),
                str(max(0, int(candidate.get("owner_chain_depth", 0) or 0))),
                *same_root_owner_dialog_titles[:4],
            ]
        )
        return {
            "status": "success",
            "backend": backend,
            "query": str(query or "").strip(),
            "window_title": str(window_title or "").strip(),
            "candidate": candidate,
            "candidates": [dict(row) for row in candidates[:8]],
            "related_windows": [dict(row) for row in related_windows[:8]],
            "owner_windows": [dict(row) for row in owner_linked_windows[:8]],
            "same_process_window_count": max(0, int(payload.get("same_process_window_count", 0) or 0)),
            "related_window_count": max(0, int(payload.get("related_window_count", len(related_windows)) or len(related_windows))),
            "owner_link_count": max(0, int(payload.get("owner_link_count", len(owner_linked_windows)) or len(owner_linked_windows))),
            "owner_chain_visible": bool(payload.get("owner_chain_visible", False)),
            "same_root_owner_window_count": max(0, int(payload.get("same_root_owner_window_count", len(same_root_owner_windows)) or len(same_root_owner_windows))),
            "same_root_owner_dialog_like_count": max(0, int(payload.get("same_root_owner_dialog_like_count", len(same_root_owner_dialog_windows)) or len(same_root_owner_dialog_windows))),
            "candidate_root_owner_hwnd": int(payload.get("candidate_root_owner_hwnd", candidate_root_owner_hwnd) or candidate_root_owner_hwnd),
            "candidate_owner_chain_depth": max(0, int(payload.get("candidate_owner_chain_depth", candidate.get("owner_chain_depth", 0)) or candidate.get("owner_chain_depth", 0) or 0)),
            "max_owner_chain_depth": max(0, int(payload.get("max_owner_chain_depth", max((int(row.get("owner_chain_depth", 0) or 0) for row in same_root_owner_windows), default=0)) or 0)),
            "child_dialog_like_visible": bool(payload.get("child_dialog_like_visible", False)),
            "owner_chain_titles": owner_chain_titles[:8],
            "same_root_owner_titles": same_root_owner_titles[:6],
            "same_root_owner_dialog_titles": same_root_owner_dialog_titles[:6],
            "modal_chain_signature": modal_chain_signature,
            "message": str(payload.get("message", "candidate_reacquired") or "candidate_reacquired").strip(),
        }

    def window_topology_snapshot(
        self,
        *,
        query: str = "",
        app_name: str = "",
        window_title: str = "",
        include_windows: bool = False,
        limit: int = 80,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 300))
        windows = self.list_windows()[:bounded]
        active = self.active_window()
        if isinstance(active, dict) and active.get("status") == "error":
            active = {}
        active = self._merge_owner_metrics(active if isinstance(active, dict) else {}, windows=windows)
        active_pid = int(active.get("pid", 0) or 0)
        active_hwnd = int(active.get("hwnd", 0) or 0)
        active_owner_hwnd = int(active.get("owner_hwnd", 0) or 0)
        active_root_owner_hwnd = int(active.get("root_owner_hwnd", active_hwnd or 0) or active_hwnd or 0)
        active_owner_chain_depth = max(0, int(active.get("owner_chain_depth", 0) or 0))
        active_signature = str(active.get("window_signature", "") or "").strip()
        active_app_name = str(active.get("app_name", "") or "").strip()
        same_process_windows = [
            dict(row)
            for row in windows
            if active_pid and int(row.get("pid", 0) or 0) == active_pid
        ]

        query_matches = [
            dict(row)
            for row in windows
            if max(
                self._text_match_score(row.get("title", ""), query),
                self._text_match_score(row.get("process_name", ""), query),
                self._text_match_score(row.get("app_name", ""), query),
            ) > 0.0
        ] if str(query or "").strip() else []
        app_matches = [
            dict(row)
            for row in windows
            if max(
                self._text_match_score(row.get("title", ""), app_name),
                self._text_match_score(row.get("process_name", ""), app_name),
                self._text_match_score(row.get("app_name", ""), app_name),
            ) > 0.0
        ] if str(app_name or "").strip() else []
        title_matches = [
            dict(row)
            for row in windows
            if self._text_match_score(row.get("title", ""), window_title) > 0.0
        ] if str(window_title or "").strip() else []

        related_windows = self._related_window_cluster(
            windows=windows,
            seed_window=active,
            app_name=app_name,
            window_title=window_title,
            query=query,
        ) if active else []
        related_hwnds = {
            int(row.get("hwnd", 0) or 0)
            for row in related_windows
            if int(row.get("hwnd", 0) or 0) > 0
        }
        owner_linked_windows = [
            dict(row)
            for row in related_windows
            if (
                active_hwnd
                and int(row.get("owner_hwnd", 0) or 0) == active_hwnd
            )
            or (
                active_owner_hwnd
                and (
                    int(row.get("hwnd", 0) or 0) == active_owner_hwnd
                    or int(row.get("owner_hwnd", 0) or 0) == active_owner_hwnd
                )
            )
            or (
                active_root_owner_hwnd
                and int(row.get("root_owner_hwnd", 0) or 0) == active_root_owner_hwnd
            )
        ]
        same_root_owner_windows = [
            dict(row)
            for row in windows
            if active_root_owner_hwnd
            and int(row.get("root_owner_hwnd", 0) or 0) == active_root_owner_hwnd
        ]
        child_dialog_like_visible = any(
            int(row.get("hwnd", 0) or 0) != active_hwnd
            and bool(row.get("surface_hints", {}).get("dialog_like", False))
            for row in related_windows
            if isinstance(row.get("surface_hints", {}), dict)
        )
        owner_link_count = len(
            {
                int(row.get("hwnd", 0) or 0)
                for row in owner_linked_windows
                if int(row.get("hwnd", 0) or 0) > 0
            }
        )
        same_root_owner_dialog_windows = [
            dict(row)
            for row in same_root_owner_windows
            if isinstance(row.get("surface_hints", {}), dict)
            and bool(row.get("surface_hints", {}).get("dialog_like", False))
        ]
        owner_chain_visible = bool(
            owner_link_count > 0
            or (active_hwnd > 0 and active_owner_hwnd > 0)
        )
        max_owner_chain_depth = max(
            [max(0, int(active.get("owner_chain_depth", 0) or 0))]
            + [max(0, int(row.get("owner_chain_depth", 0) or 0)) for row in same_root_owner_windows]
        ) if active else 0
        owner_chain_titles = [
            str(row.get("title", "") or "").strip()
            for row in self._owner_chain_rows(active, windows=windows)
            if str(row.get("title", "") or "").strip()
        ]
        topology_backend = str(
            active.get("observation_backend", "")
            or (windows[0].get("observation_backend", "") if windows else "")
            or "pywin32"
        ).strip() or "pywin32"
        topology_signature = "|".join(
            [
                active_app_name or "unknown",
                str(len(windows)),
                str(len(same_process_windows)),
                str(
                    sum(
                        1
                        for row in windows
                        if isinstance(row.get("surface_hints", {}), dict)
                        and bool(row.get("surface_hints", {}).get("dialog_like", False))
                    )
                ),
                str(len(related_hwnds)),
                str(owner_link_count),
                str(len(same_root_owner_windows)),
                str(max_owner_chain_depth),
                str(len(query_matches)),
            ]
        )
        payload = {
            "status": "success",
            "backend": topology_backend,
            "query": str(query or "").strip(),
            "app_name": str(app_name or "").strip(),
            "window_title": str(window_title or "").strip(),
            "active_window": active,
            "active_hwnd": active_hwnd,
            "active_pid": active_pid,
            "active_app_name": active_app_name,
            "active_window_signature": active_signature,
            "visible_window_count": len(windows),
            "dialog_like_count": sum(
                1
                for row in windows
                if isinstance(row.get("surface_hints", {}), dict) and bool(row.get("surface_hints", {}).get("dialog_like", False))
            ),
            "same_process_window_count": len(same_process_windows),
            "query_match_count": len(query_matches),
            "app_match_count": len(app_matches),
            "title_match_count": len(title_matches),
            "related_window_count": len(related_windows),
            "owner_link_count": owner_link_count,
            "owner_chain_visible": owner_chain_visible,
            "same_root_owner_window_count": len(same_root_owner_windows),
            "same_root_owner_dialog_like_count": len(same_root_owner_dialog_windows),
            "active_owner_chain_depth": active_owner_chain_depth,
            "max_owner_chain_depth": max_owner_chain_depth,
            "child_dialog_like_visible": bool(child_dialog_like_visible),
            "same_process_titles": [str(row.get("title", "") or "").strip() for row in same_process_windows[:6] if str(row.get("title", "") or "").strip()],
            "related_window_titles": [str(row.get("title", "") or "").strip() for row in related_windows[:6] if str(row.get("title", "") or "").strip()],
            "owner_window_titles": [str(row.get("title", "") or "").strip() for row in owner_linked_windows[:6] if str(row.get("title", "") or "").strip()],
            "same_root_owner_titles": [str(row.get("title", "") or "").strip() for row in same_root_owner_windows[:6] if str(row.get("title", "") or "").strip()],
            "same_root_owner_dialog_titles": [str(row.get("title", "") or "").strip() for row in same_root_owner_dialog_windows[:6] if str(row.get("title", "") or "").strip()],
            "owner_chain_titles": owner_chain_titles[:8],
            "modal_chain_signature": "|".join(
                [
                    str(active_root_owner_hwnd or 0),
                    str(len(same_root_owner_dialog_windows)),
                    str(active_owner_chain_depth),
                    *[
                        str(row.get("title", "") or "").strip()
                        for row in same_root_owner_dialog_windows[:4]
                        if str(row.get("title", "") or "").strip()
                    ],
                ]
            ),
            "topology_signature": topology_signature,
        }
        if include_windows:
            payload["windows"] = [dict(row) for row in windows[:12]]
            payload["related_windows"] = [dict(row) for row in related_windows[:8]]
            payload["owner_windows"] = [dict(row) for row in owner_linked_windows[:8]]
        return payload

    def reacquire_window(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        window_signature: str = "",
        hwnd: int | None = None,
        pid: int | None = None,
        parent_hwnd: int | None = None,
        parent_pid: int | None = None,
        include_candidates: bool = True,
        limit: int = 80,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 300))
        native_payload = self._native_reacquire_window(
            query=query,
            window_title=window_title,
            hwnd=hwnd,
            pid=pid,
            limit=bounded,
        )
        if native_payload is not None:
            native_payload["app_name"] = str(app_name or "").strip()
            native_payload["window_signature"] = str(window_signature or "").strip()
            return native_payload
        windows = self.list_windows()[:bounded]
        anchor_window = next(
            (
                dict(row)
                for row in windows
                if int(row.get("hwnd", 0) or 0) == int(hwnd or 0)
            ),
            {},
        )
        ranked: List[tuple[float, Dict[str, Any]]] = []
        for row in windows:
            relation = self._window_relation_score(
                window=row,
                query=query,
                app_name=app_name,
                window_title=window_title,
                window_signature=window_signature,
                hwnd=int(hwnd or 0),
                root_owner_hwnd=int(anchor_window.get("root_owner_hwnd", hwnd or 0) or hwnd or 0),
                owner_chain_depth=max(0, int(anchor_window.get("owner_chain_depth", 0) or 0)),
                pid=int(pid or 0),
                parent_hwnd=int(parent_hwnd or 0),
                parent_pid=int(parent_pid or 0),
            )
            relation_score = float(relation.get("score", 0.0) or 0.0)
            if relation_score <= 0.0:
                continue
            enriched = dict(row)
            enriched["match_score"] = round(relation_score, 4)
            enriched["match_reasons"] = list(relation.get("reasons", []))
            ranked.append((relation_score, enriched))
        ranked.sort(
            key=lambda item: (
                -item[0],
                not bool(item[1].get("is_foreground", False)),
                -int(item[1].get("area", 0) or 0),
                str(item[1].get("title", "") or "").lower(),
            )
        )
        candidates = [row for _score, row in ranked[:8]]
        candidate = dict(candidates[0]) if candidates else {}
        status = "success" if candidate and float(candidate.get("match_score", 0.0) or 0.0) >= 0.42 else "missing"
        related_windows = self._related_window_cluster(
            windows=windows,
            seed_window=candidate or (self.active_window() if isinstance(self.active_window(), dict) else {}),
            app_name=app_name,
            window_title=window_title,
            query=query,
        ) if candidate else []
        candidate_hwnd = int(candidate.get("hwnd", 0) or 0) if candidate else 0
        candidate_owner_hwnd = int(candidate.get("owner_hwnd", 0) or 0) if candidate else 0
        owner_linked_windows = [
            dict(row)
            for row in related_windows
            if (
                candidate_hwnd
                and int(row.get("owner_hwnd", 0) or 0) == candidate_hwnd
            )
            or (
                candidate_owner_hwnd
                and (
                    int(row.get("hwnd", 0) or 0) == candidate_owner_hwnd
                    or int(row.get("owner_hwnd", 0) or 0) == candidate_owner_hwnd
                )
            )
            or (
                candidate
                and int(candidate.get("root_owner_hwnd", 0) or 0)
                and int(row.get("root_owner_hwnd", 0) or 0) == int(candidate.get("root_owner_hwnd", 0) or 0)
            )
        ] if candidate else []
        same_root_owner_windows = [
            dict(row)
            for row in windows
            if candidate
            and int(candidate.get("root_owner_hwnd", 0) or 0)
            and int(row.get("root_owner_hwnd", 0) or 0) == int(candidate.get("root_owner_hwnd", 0) or 0)
        ] if candidate else []
        owner_link_count = len(
            {
                int(row.get("hwnd", 0) or 0)
                for row in owner_linked_windows
                if int(row.get("hwnd", 0) or 0) > 0
            }
        ) if candidate else 0
        same_root_owner_dialog_windows = [
            dict(row)
            for row in same_root_owner_windows
            if isinstance(row.get("surface_hints", {}), dict)
            and bool(row.get("surface_hints", {}).get("dialog_like", False))
        ] if candidate else []
        owner_chain_titles = [
            str(row.get("title", "") or "").strip()
            for row in self._owner_chain_rows(candidate, windows=windows)
            if str(row.get("title", "") or "").strip()
        ] if candidate else []
        max_owner_chain_depth = max(
            [max(0, int(candidate.get("owner_chain_depth", 0) or 0))]
            + [max(0, int(row.get("owner_chain_depth", 0) or 0)) for row in same_root_owner_windows]
        ) if candidate else 0
        payload = {
            "status": status,
            "backend": str(candidate.get("observation_backend", "") or "pywin32"),
            "query": str(query or "").strip(),
            "app_name": str(app_name or "").strip(),
            "window_title": str(window_title or "").strip(),
            "window_signature": str(window_signature or "").strip(),
            "candidate": candidate,
            "related_window_count": len(related_windows),
            "owner_link_count": owner_link_count,
            "owner_chain_visible": bool(
                candidate
                and (
                    owner_link_count > 0
                    or (candidate_hwnd > 0 and candidate_owner_hwnd > 0)
                )
            ),
            "same_root_owner_window_count": len(same_root_owner_windows),
            "same_root_owner_dialog_like_count": len(same_root_owner_dialog_windows),
            "candidate_root_owner_hwnd": int(candidate.get("root_owner_hwnd", 0) or 0) if candidate else 0,
            "candidate_owner_chain_depth": max(0, int(candidate.get("owner_chain_depth", 0) or 0)) if candidate else 0,
            "max_owner_chain_depth": max_owner_chain_depth,
            "same_process_window_count": len(
                [
                    row
                    for row in windows
                    if candidate and int(candidate.get("pid", 0) or 0) and int(row.get("pid", 0) or 0) == int(candidate.get("pid", 0) or 0)
                ]
            ) if candidate else 0,
            "child_dialog_like_visible": any(
                int(row.get("hwnd", 0) or 0) != int(candidate.get("hwnd", 0) or 0)
                and isinstance(row.get("surface_hints", {}), dict)
                and bool(row.get("surface_hints", {}).get("dialog_like", False))
                for row in related_windows
            ) if candidate else False,
            "message": "candidate_reacquired" if status == "success" else "no matching related window candidate found",
            "owner_chain_titles": owner_chain_titles[:8],
            "same_root_owner_titles": [str(row.get("title", "") or "").strip() for row in same_root_owner_windows[:6] if str(row.get("title", "") or "").strip()],
            "same_root_owner_dialog_titles": [str(row.get("title", "") or "").strip() for row in same_root_owner_dialog_windows[:6] if str(row.get("title", "") or "").strip()],
            "modal_chain_signature": "|".join(
                [
                    str(int(candidate.get("root_owner_hwnd", 0) or 0) if candidate else 0),
                    str(len(same_root_owner_dialog_windows)),
                    str(max(0, int(candidate.get("owner_chain_depth", 0) or 0)) if candidate else 0),
                    *[
                        str(row.get("title", "") or "").strip()
                        for row in same_root_owner_dialog_windows[:4]
                        if str(row.get("title", "") or "").strip()
                    ],
                ]
            ),
        }
        if include_candidates:
            payload["candidates"] = [dict(row) for row in candidates]
            payload["related_windows"] = [dict(row) for row in related_windows]
            payload["owner_windows"] = [dict(row) for row in owner_linked_windows]
        return payload

    def _get_hwnd_info(self, hwnd: int) -> Dict[str, Any]:
        if win32gui is None or win32process is None:
            return {}
        try:
            title = win32gui.GetWindowText(hwnd)
            rect = win32gui.GetWindowRect(hwnd)
            class_name = win32gui.GetClassName(hwnd)
            visible = bool(win32gui.IsWindowVisible(hwnd))
            enabled = bool(win32gui.IsWindowEnabled(hwnd))
            minimized = bool(win32gui.IsIconic(hwnd))
            maximized = bool(win32gui.IsZoomed(hwnd))
            foreground_hwnd = win32gui.GetForegroundWindow()
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            owner_hwnd = int(win32gui.GetWindow(hwnd, getattr(win32con, "GW_OWNER", 4)) or 0)
            proc = psutil.Process(pid)
            exe = proc.exe() if proc else None
            process_name = proc.name() if proc else None
            left, top, right, bottom = rect
            return self._compose_window_info(
                {
                    "hwnd": hwnd,
                    "owner_hwnd": owner_hwnd,
                    "title": title,
                    "pid": pid,
                    "exe": exe,
                    "process_name": process_name,
                    "class_name": class_name,
                    "visible": visible,
                    "enabled": enabled,
                    "minimized": minimized,
                    "maximized": maximized,
                    "is_foreground": bool(hwnd == foreground_hwnd),
                    "left": left,
                    "top": top,
                    "right": right,
                    "bottom": bottom,
                },
                observation_backend="pywin32",
            )
        except Exception:
            return {}

    def list_windows(self) -> List[Dict[str, Any]]:
        native_windows = self._native_list_windows(limit=300)
        if native_windows is not None:
            return native_windows
        if win32gui is None:
            return []
        windows: List[Dict[str, Any]] = []

        def callback(hwnd, _):
            if win32gui.IsWindowVisible(hwnd) and win32gui.GetWindowText(hwnd):
                info = self._get_hwnd_info(hwnd)
                if info:
                    windows.append(info)
            return True

        win32gui.EnumWindows(callback, None)
        return self._enrich_owner_chain_metrics(windows)

    def active_window(self) -> Dict[str, Any]:
        native_window = self._native_active_window()
        if native_window is not None:
            return native_window
        if win32gui is None:
            return {"status": "error", "message": "pywin32 is not available"}
        try:
            hwnd = win32gui.GetForegroundWindow()
            return self._merge_owner_metrics(self._get_hwnd_info(hwnd), windows=self.list_windows())
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def get_active_window(self) -> Dict[str, Any]:
        """Compatibility alias used by higher-level context monitoring."""
        return self.active_window()

    def describe_window(self, *, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any]:
        """Return a richer view of a specific or active window without changing focus."""
        if hwnd is not None:
            payload = next((item for item in self.list_windows() if int(item.get("hwnd", 0) or 0) == int(hwnd)), {})
            if not payload:
                payload = self._get_hwnd_info(int(hwnd))
            return {"status": "success", "window": payload} if payload else {"status": "error", "message": "Window not found"}

        if title_contains:
            needle = self._normalize_text(title_contains)
            for item in self.list_windows():
                if needle and needle in self._normalize_text(item.get("title", "")):
                    return {"status": "success", "window": item}
            return {"status": "error", "message": "Window not found"}

        active = self.active_window()
        if isinstance(active, dict) and active.get("status") == "error":
            return active
        return {"status": "success", "window": active}

    def focus_window(self, title_contains: str = "", hwnd: int | None = None) -> Dict[str, Any]:
        native_result = self._native_focus_window(title_contains=title_contains, hwnd=hwnd)
        if native_result is not None:
            return native_result
        if win32gui is None:
            return {"status": "error", "message": "pywin32 is not available"}

        target_hwnd: int | None = None
        if hwnd is not None:
            try:
                if win32gui.IsWindow(hwnd):
                    target_hwnd = hwnd
            except Exception:
                target_hwnd = None

        if target_hwnd is None:
            needle = title_contains.strip().lower()
            if not needle:
                return {"status": "error", "message": "title_contains or hwnd is required"}

            found: list[int] = []

            def callback(candidate_hwnd: int, _: object) -> bool:
                try:
                    if not win32gui.IsWindowVisible(candidate_hwnd):
                        return True
                    title = win32gui.GetWindowText(candidate_hwnd)
                    if title and needle in title.lower():
                        found.append(candidate_hwnd)
                        return False
                except Exception:
                    return True
                return True

            win32gui.EnumWindows(callback, None)
            target_hwnd = found[0] if found else None

        if target_hwnd is None:
            return {"status": "error", "message": "Window not found"}

        try:
            # 5 = SW_SHOW
            win32gui.ShowWindow(target_hwnd, 5)
            win32gui.SetForegroundWindow(target_hwnd)
            return {"status": "success", "window": self._get_hwnd_info(target_hwnd)}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
