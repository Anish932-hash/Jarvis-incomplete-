from __future__ import annotations

import hashlib
import time
from threading import RLock
from typing import Any, Dict, List, Optional


class AccessibilityTools:
    """
    Windows accessibility utilities (UI Automation), with graceful degradation.
    """

    _cache: Dict[str, Dict[str, Any]] = {}
    _cache_lock = RLock()

    @staticmethod
    def health() -> Dict[str, Any]:
        pywinauto = AccessibilityTools._pywinauto_desktop()
        status = "success" if pywinauto is not None else "degraded"
        return {
            "status": status,
            "provider": "pywinauto_uia" if pywinauto is not None else "unavailable",
            "capabilities": {
                "list_elements": pywinauto is not None,
                "find_element": pywinauto is not None,
                "invoke_element": pywinauto is not None,
            },
        }

    @classmethod
    def list_elements(
        cls,
        *,
        window_title: str = "",
        query: str = "",
        control_type: str = "",
        include_descendants: bool = True,
        max_elements: int = 150,
    ) -> Dict[str, Any]:
        desktop = cls._pywinauto_desktop()
        if desktop is None:
            return {"status": "error", "message": "pywinauto is unavailable."}

        title_filter = str(window_title or "").strip().lower()
        query_filter = str(query or "").strip().lower()
        type_filter = str(control_type or "").strip().lower()
        bounded = max(1, min(int(max_elements), 1000))

        rows: List[Dict[str, Any]] = []
        cache_rows: Dict[str, Dict[str, Any]] = {}
        try:
            windows = desktop.windows()
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

        for win in windows:
            serialized = cls._serialize_element(win, parent_id="")
            if not cls._matches(serialized, title_filter=title_filter, query_filter=query_filter, type_filter=type_filter):
                pass
            else:
                rows.append(serialized)
                cache_rows[serialized["element_id"]] = serialized
                if len(rows) >= bounded:
                    break

            if len(rows) >= bounded or not include_descendants:
                continue
            try:
                descendants = win.descendants()
            except Exception:
                descendants = []
            for child in descendants:
                serialized_child = cls._serialize_element(child, parent_id=serialized.get("element_id", ""))
                if not cls._matches(
                    serialized_child,
                    title_filter=title_filter,
                    query_filter=query_filter,
                    type_filter=type_filter,
                ):
                    continue
                rows.append(serialized_child)
                cache_rows[serialized_child["element_id"]] = serialized_child
                if len(rows) >= bounded:
                    break
            if len(rows) >= bounded:
                break

        with cls._cache_lock:
            cls._cache.update(cache_rows)
            if len(cls._cache) > 8000:
                keys = list(cls._cache.keys())[-5000:]
                cls._cache = {key: cls._cache[key] for key in keys if key in cls._cache}

        return {
            "status": "success",
            "count": len(rows),
            "items": rows,
            "window_title_filter": window_title,
            "query_filter": query,
            "control_type_filter": control_type,
        }

    @classmethod
    def find_element(
        cls,
        *,
        query: str,
        window_title: str = "",
        control_type: str = "",
        max_results: int = 10,
    ) -> Dict[str, Any]:
        phrase = str(query or "").strip()
        if not phrase:
            return {"status": "error", "message": "query is required"}

        listed = cls.list_elements(
            window_title=window_title,
            query=phrase,
            control_type=control_type,
            include_descendants=True,
            max_elements=max(30, min(int(max_results) * 25, 700)),
        )
        if listed.get("status") != "success":
            return listed

        items = listed.get("items", [])
        if not isinstance(items, list):
            items = []
        lowered = phrase.lower()
        ranked: List[tuple[float, Dict[str, Any]]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            item_type = str(item.get("control_type", "")).strip().lower()
            haystack = name.lower()
            if not haystack:
                continue

            if haystack == lowered:
                score = 1.0
            elif lowered in haystack:
                score = 0.8 + min(0.15, len(lowered) / max(1.0, len(haystack)))
            else:
                query_tokens = {token for token in lowered.split() if token}
                name_tokens = {token for token in haystack.split() if token}
                overlap = len(query_tokens.intersection(name_tokens))
                if overlap <= 0:
                    continue
                score = overlap / max(1.0, len(query_tokens))
            if control_type and item_type == control_type.lower():
                score += 0.05
            ranked.append((score, dict(item, match_score=round(score, 6))))

        ranked.sort(key=lambda row: row[0], reverse=True)
        bounded = max(1, min(int(max_results), 100))
        matches = [item for _, item in ranked[:bounded]]
        return {"status": "success", "query": phrase, "count": len(matches), "items": matches}

    @classmethod
    def invoke_element(
        cls,
        *,
        element_id: str = "",
        query: str = "",
        action: str = "click",
        window_title: str = "",
        control_type: str = "",
        click_offset_x: int = 0,
        click_offset_y: int = 0,
    ) -> Dict[str, Any]:
        op = str(action or "click").strip().lower() or "click"
        if op not in {"click", "double_click", "right_click", "focus"}:
            return {"status": "error", "message": "action must be click, double_click, right_click, or focus"}

        target = cls._resolve_target(
            element_id=str(element_id or "").strip(),
            query=str(query or "").strip(),
            window_title=str(window_title or "").strip(),
            control_type=str(control_type or "").strip(),
        )
        if target is None:
            return {"status": "error", "message": "UI element not found"}

        handle = cls._to_int(target.get("handle"))
        if op == "focus" and handle:
            desktop = cls._pywinauto_desktop()
            if desktop is None:
                return {"status": "error", "message": "pywinauto is unavailable for focus action"}
            try:
                desktop.window(handle=handle).set_focus()
                return {"status": "success", "action": op, "element": target}
            except Exception as exc:  # noqa: BLE001
                return {"status": "error", "message": str(exc)}

        center_x = cls._to_int(target.get("center_x"))
        center_y = cls._to_int(target.get("center_y"))
        if center_x is None or center_y is None:
            return {"status": "error", "message": "Element does not have clickable bounds"}

        x = center_x + int(click_offset_x)
        y = center_y + int(click_offset_y)
        pyautogui = cls._import_pyautogui()
        if pyautogui is None:
            return {"status": "error", "message": "pyautogui is unavailable"}

        try:
            if op == "double_click":
                pyautogui.click(x=x, y=y, clicks=2, interval=0.07, button="left")
            elif op == "right_click":
                pyautogui.click(x=x, y=y, button="right")
            else:
                pyautogui.click(x=x, y=y, button="left")
            return {
                "status": "success",
                "action": op,
                "x": x,
                "y": y,
                "element": target,
                "clicked_at": datetime_now_iso(),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _resolve_target(
        cls,
        *,
        element_id: str,
        query: str,
        window_title: str,
        control_type: str,
    ) -> Optional[Dict[str, Any]]:
        if element_id:
            with cls._cache_lock:
                cached = cls._cache.get(element_id)
            if isinstance(cached, dict):
                return dict(cached)

        if query:
            found = cls.find_element(
                query=query,
                window_title=window_title,
                control_type=control_type,
                max_results=1,
            )
            if found.get("status") != "success":
                return None
            items = found.get("items", [])
            if isinstance(items, list) and items:
                return items[0] if isinstance(items[0], dict) else None
        return None

    @staticmethod
    def _matches(
        row: Dict[str, Any],
        *,
        title_filter: str,
        query_filter: str,
        type_filter: str,
    ) -> bool:
        name = str(row.get("name", "")).strip().lower()
        window_title = str(row.get("window_title", "")).strip().lower()
        control_type = str(row.get("control_type", "")).strip().lower()
        if title_filter and title_filter not in window_title:
            return False
        if query_filter and query_filter not in name:
            return False
        if type_filter and type_filter != control_type:
            return False
        return True

    @staticmethod
    def _serialize_element(element: Any, *, parent_id: str) -> Dict[str, Any]:
        name = ""
        handle = None
        control_type = ""
        class_name = ""
        auto_id = ""
        enabled = None
        visible = None
        window_title = ""
        left = top = width = height = center_x = center_y = None

        try:
            name = str(element.window_text() or "").strip()
        except Exception:
            name = ""
        try:
            handle = int(getattr(element, "handle", 0) or 0) or None
        except Exception:
            handle = None
        try:
            rect = element.rectangle()
            left = int(rect.left)
            top = int(rect.top)
            width = int(max(0, rect.width()))
            height = int(max(0, rect.height()))
            center_x = left + (width // 2)
            center_y = top + (height // 2)
        except Exception:
            pass
        try:
            info = element.element_info
            control_type = str(getattr(info, "control_type", "") or "").strip()
            auto_id = str(getattr(info, "automation_id", "") or "").strip()
            class_name = str(getattr(info, "class_name", "") or "").strip()
            window_title = str(getattr(info, "name", "") or "").strip()
        except Exception:
            pass
        try:
            enabled = bool(element.is_enabled())
        except Exception:
            enabled = None
        try:
            visible = bool(element.is_visible())
        except Exception:
            visible = None
        if not window_title:
            window_title = name

        raw_key = f"{handle}|{name}|{control_type}|{auto_id}|{class_name}|{left}|{top}|{width}|{height}"
        digest = hashlib.sha1(raw_key.encode("utf-8", errors="ignore")).hexdigest()[:16]
        element_id = f"uia_{digest}"

        payload = {
            "element_id": element_id,
            "parent_id": parent_id,
            "name": name,
            "window_title": window_title,
            "control_type": control_type,
            "class_name": class_name,
            "automation_id": auto_id,
            "handle": handle,
            "enabled": enabled,
            "visible": visible,
            "left": left,
            "top": top,
            "width": width,
            "height": height,
            "center_x": center_x,
            "center_y": center_y,
            "captured_at": datetime_now_iso(),
        }
        return {key: value for key, value in payload.items() if value not in {None, ""}}

    @staticmethod
    def _pywinauto_desktop():
        try:
            from pywinauto import Desktop  # type: ignore

            return Desktop(backend="uia")
        except Exception:
            return None

    @staticmethod
    def _import_pyautogui():
        try:
            import pyautogui  # type: ignore

            pyautogui.FAILSAFE = True
            pyautogui.PAUSE = 0.03
            return pyautogui
        except Exception:
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None


def datetime_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
