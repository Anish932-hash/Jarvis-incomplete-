from __future__ import annotations

from collections import Counter
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
                serialized_child = cls._serialize_element(
                    child,
                    parent_id=serialized.get("element_id", ""),
                    window_title_hint=str(serialized.get("window_title", "") or serialized.get("name", "")).strip(),
                    root_handle=cls._to_int(serialized.get("handle")) or cls._to_int(serialized.get("root_handle")),
                )
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
        matches = cls._rank_query_candidates(
            rows=items,
            query=phrase,
            control_type=control_type,
            max_results=max_results,
        )
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
    def surface_summary(
        cls,
        *,
        window_title: str = "",
        query: str = "",
        max_elements: int = 220,
        include_inventory: bool = True,
    ) -> Dict[str, Any]:
        bounded = max(20, min(int(max_elements), 1000))
        listed = cls.list_elements(
            window_title=window_title,
            query="",
            include_descendants=True,
            max_elements=bounded,
        )
        if listed.get("status") != "success":
            return listed

        items = [item for item in listed.get("items", []) if isinstance(item, dict)]
        return cls.summarize_rows(
            rows=items,
            window_title=window_title,
            query=query,
            include_inventory=include_inventory,
        )

    @classmethod
    def summarize_rows(
        cls,
        *,
        rows: List[Dict[str, Any]],
        window_title: str = "",
        query: str = "",
        include_inventory: bool = True,
    ) -> Dict[str, Any]:
        items = [dict(item) for item in rows if isinstance(item, dict)]
        control_counts: Counter[str] = Counter()
        state_counts: Counter[str] = Counter()
        label_counts: Counter[str] = Counter()
        actionable_candidates = 0
        input_controls = 0
        selection_controls = 0
        value_controls = 0
        inventory: List[Dict[str, Any]] = []

        for item in items:
            control_type = str(item.get("control_type", "") or "unknown").strip().lower()
            control_counts[control_type or "unknown"] += 1
            if item.get("selected") is True:
                state_counts["selected"] += 1
            if item.get("checked") is True:
                state_counts["checked"] += 1
            if item.get("expanded") is True:
                state_counts["expanded"] += 1
            toggle_state = str(item.get("toggle_state", "") or "").strip().lower()
            if toggle_state:
                state_counts[f"toggle:{toggle_state}"] += 1

            if control_type in {"edit", "document", "text", "combobox", "combo box", "spinner"}:
                input_controls += 1
            if control_type in {"treeitem", "listitem", "tabitem", "dataitem", "row", "checkbox", "radiobutton"}:
                selection_controls += 1
            if item.get("range_value") is not None or control_type in {"slider", "spinner", "progressbar"}:
                value_controls += 1
            if control_type in {
                "button",
                "menuitem",
                "hyperlink",
                "checkbox",
                "radiobutton",
                "treeitem",
                "listitem",
                "tabitem",
                "dataitem",
                "slider",
                "combobox",
            }:
                actionable_candidates += 1

            label = str(item.get("name", "") or item.get("automation_id", "") or "").strip()
            if label:
                label_counts[label.lower()] += 1

            if include_inventory and len(inventory) < 40:
                inventory.append(
                    {
                        "element_id": item.get("element_id"),
                        "name": item.get("name", ""),
                        "control_type": item.get("control_type", ""),
                        "automation_id": item.get("automation_id", ""),
                        "state_text": item.get("state_text", ""),
                        "root_window_title": item.get("root_window_title", item.get("window_title", "")),
                    }
                )

        query_candidates: List[Dict[str, Any]] = []
        if str(query or "").strip():
            query_candidates = cls._rank_query_candidates(
                rows=items,
                query=str(query or "").strip(),
                max_results=6,
            )
            if (
                window_title
                and (
                    not query_candidates
                    or any(not str(item.get("element_id", "") or "").strip() for item in query_candidates)
                )
            ):
                found = cls.find_element(
                    query=str(query or "").strip(),
                    window_title=window_title,
                    max_results=6,
                )
                if found.get("status") == "success":
                    found_rows = [row for row in found.get("items", []) if isinstance(row, dict)]
                    found_by_fallback: Dict[str, Dict[str, Any]] = {}

                    def _is_blank(value: Any) -> bool:
                        if value is None:
                            return True
                        if isinstance(value, str):
                            return not value.strip()
                        if isinstance(value, (list, dict, tuple, set)):
                            return len(value) == 0
                        return False

                    for row in found_rows:
                        fallback_identity = "|".join(
                            [
                                str(row.get("name", "") or "").strip().lower(),
                                str(row.get("control_type", "") or "").strip().lower(),
                                str(row.get("automation_id", "") or "").strip().lower(),
                            ]
                        )
                        if fallback_identity and fallback_identity not in found_by_fallback:
                            found_by_fallback[fallback_identity] = dict(row)
                    enriched_query_candidates: List[Dict[str, Any]] = []
                    for row in query_candidates:
                        candidate = dict(row)
                        if not str(candidate.get("element_id", "") or "").strip():
                            fallback_identity = "|".join(
                                [
                                    str(candidate.get("name", "") or "").strip().lower(),
                                    str(candidate.get("control_type", "") or "").strip().lower(),
                                    str(candidate.get("automation_id", "") or "").strip().lower(),
                                ]
                            )
                            matched_row = found_by_fallback.get(fallback_identity)
                            if matched_row:
                                for key, value in matched_row.items():
                                    if _is_blank(candidate.get(key)) and not _is_blank(value):
                                        candidate[key] = value
                        enriched_query_candidates.append(candidate)
                    merged_candidates: List[Dict[str, Any]] = []
                    seen_candidates: set[str] = set()
                    for item in [*enriched_query_candidates, *found_rows]:
                        identity = (
                            str(item.get("element_id", "") or "").strip()
                            or "|".join(
                                [
                                    str(item.get("name", "") or "").strip().lower(),
                                    str(item.get("control_type", "") or "").strip().lower(),
                                    str(item.get("automation_id", "") or "").strip().lower(),
                                ]
                            )
                        )
                        if not identity or identity in seen_candidates:
                            continue
                        seen_candidates.add(identity)
                        merged_candidates.append(item)
                        if len(merged_candidates) >= 6:
                            break
                    query_candidates = merged_candidates

        flags = cls._surface_flags_from_rows(items, control_counts=control_counts)
        role_candidates = cls._surface_role_candidates(flags=flags, control_counts=control_counts)
        recommended_actions = cls._surface_recommendations(flags=flags, query_candidates=query_candidates)

        destructive_candidates = [
            row.get("name", "")
            for row in inventory
            if any(
                token in str(row.get("name", "")).strip().lower()
                for token in ("delete", "remove", "uninstall", "reset", "disable", "erase", "format")
            )
        ]
        confirmation_candidates = [
            row.get("name", "")
            for row in inventory
            if any(
                token in str(row.get("name", "")).strip().lower()
                for token in ("ok", "apply", "save", "continue", "next", "install", "confirm", "allow")
            )
        ]

        summary_parts: List[str] = []
        if role_candidates:
            summary_parts.append(f"looks like {role_candidates[0]}")
        if flags.get("navigation_tree_visible"):
            summary_parts.append("tree navigation available")
        if flags.get("list_surface_visible"):
            summary_parts.append("list selection available")
        if flags.get("form_surface_visible"):
            summary_parts.append("form controls visible")
        if flags.get("dialog_visible"):
            summary_parts.append("dialog-like surface")
        if query_candidates:
            summary_parts.append(f"{len(query_candidates)} query candidates found")

        return {
            "status": "success",
            "window_title_filter": window_title,
            "query": query,
            "element_count": len(items),
            "control_counts": dict(sorted(control_counts.items())),
            "state_counts": dict(sorted(state_counts.items())),
            "surface_flags": flags,
            "surface_role_candidates": role_candidates,
            "actionable_candidate_count": actionable_candidates,
            "input_control_count": input_controls,
            "selection_control_count": selection_controls,
            "value_control_count": value_controls,
            "top_labels": [{"label": name, "count": count} for name, count in label_counts.most_common(12)],
            "query_candidates": query_candidates,
            "recommended_actions": recommended_actions,
            "destructive_candidates": [name for name in destructive_candidates if name],
            "confirmation_candidates": [name for name in confirmation_candidates if name],
            "control_inventory": inventory if include_inventory else [],
            "summary": "; ".join(summary_parts) if summary_parts else "surface summary unavailable",
        }

    @classmethod
    def _rank_query_candidates(
        cls,
        *,
        rows: List[Dict[str, Any]],
        query: str,
        control_type: str = "",
        max_results: int = 10,
    ) -> List[Dict[str, Any]]:
        lowered = str(query or "").strip().lower()
        if not lowered:
            return []

        ranked: List[tuple[float, Dict[str, Any]]] = []
        type_filter = str(control_type or "").strip().lower()
        for item in rows:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            item_type = str(item.get("control_type", "")).strip().lower()
            haystack = cls._search_text(item)
            name_haystack = " ".join(name.lower().split())
            automation_haystack = " ".join(str(item.get("automation_id", "") or "").strip().lower().split())
            state_haystack = " ".join(str(item.get("state_text", "") or "").strip().lower().split())
            value_haystack = " ".join(str(item.get("value_text", "") or "").strip().lower().split())
            if not haystack:
                continue

            if name_haystack == lowered or haystack == lowered:
                score = 1.0
            elif lowered in name_haystack:
                score = 0.88 + min(0.1, len(lowered) / max(1.0, len(name_haystack)))
            elif lowered in haystack:
                score = 0.76 + min(0.18, len(lowered) / max(1.0, len(haystack)))
            else:
                query_tokens = {token for token in lowered.split() if token}
                name_tokens = {token for token in haystack.split() if token}
                overlap = len(query_tokens.intersection(name_tokens))
                if overlap <= 0:
                    continue
                score = overlap / max(1.0, len(query_tokens))
            if lowered and any(lowered in field for field in (automation_haystack, state_haystack, value_haystack) if field):
                score += 0.04
            if type_filter and item_type == type_filter:
                score += 0.05
            ranked.append((score, dict(item, match_score=round(score, 6))))

        ranked.sort(key=lambda row: row[0], reverse=True)
        bounded = max(1, min(int(max_results), 100))
        return [item for _, item in ranked[:bounded]]

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
        window_title = str(row.get("window_title", "")).strip().lower()
        root_window_title = str(row.get("root_window_title", "")).strip().lower()
        control_type = str(row.get("control_type", "")).strip().lower()
        query_haystack = AccessibilityTools._search_text(row)
        if title_filter and title_filter not in window_title and title_filter not in root_window_title:
            return False
        if query_filter and query_filter not in query_haystack:
            return False
        if type_filter and type_filter != control_type:
            return False
        return True

    @staticmethod
    def _serialize_element(
        element: Any,
        *,
        parent_id: str,
        window_title_hint: str = "",
        root_handle: Optional[int] = None,
    ) -> Dict[str, Any]:
        name = ""
        handle = None
        control_type = ""
        class_name = ""
        auto_id = ""
        enabled = None
        visible = None
        window_title = ""
        root_window_title = str(window_title_hint or "").strip()
        left = top = width = height = center_x = center_y = None
        selected = None
        checked = None
        toggle_state = ""
        expanded = None
        value_text = ""
        state_text = ""
        range_value = None
        range_min = None
        range_max = None
        properties: Dict[str, Any] = {}

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
        properties_raw = AccessibilityTools._safe_call(element, "get_properties")
        if isinstance(properties_raw, dict):
            properties = dict(properties_raw)
        selected_raw = AccessibilityTools._first_present(
            properties.get("is_selected"),
            properties.get("selected"),
            AccessibilityTools._safe_call(element, "is_selected"),
            AccessibilityTools._safe_path(element, "iface_selection_item", "CurrentIsSelected"),
        )
        checked_raw = AccessibilityTools._first_present(
            properties.get("is_checked"),
            properties.get("checked"),
            AccessibilityTools._safe_call(element, "is_checked"),
        )
        toggle_state_raw = AccessibilityTools._first_present(
            properties.get("toggle_state"),
            AccessibilityTools._safe_call(element, "get_toggle_state"),
            AccessibilityTools._safe_path(element, "iface_toggle", "CurrentToggleState"),
        )
        expanded_raw = AccessibilityTools._first_present(
            properties.get("is_expanded"),
            properties.get("expanded"),
            AccessibilityTools._safe_call(element, "is_expanded"),
            AccessibilityTools._safe_path(element, "iface_expand_collapse", "CurrentExpandCollapseState"),
        )
        value_raw = AccessibilityTools._first_present(
            properties.get("value"),
            properties.get("legacy_value"),
            AccessibilityTools._safe_call(element, "get_value"),
            AccessibilityTools._safe_path(element, "iface_value", "CurrentValue"),
            AccessibilityTools._safe_path(element, "iface_range_value", "CurrentValue"),
        )
        range_value_raw = AccessibilityTools._first_present(
            properties.get("range_value"),
            AccessibilityTools._safe_path(element, "iface_range_value", "CurrentValue"),
        )
        range_min_raw = AccessibilityTools._first_present(
            properties.get("range_min"),
            AccessibilityTools._safe_path(element, "iface_range_value", "CurrentMinimum"),
        )
        range_max_raw = AccessibilityTools._first_present(
            properties.get("range_max"),
            AccessibilityTools._safe_path(element, "iface_range_value", "CurrentMaximum"),
        )
        selected = AccessibilityTools._coerce_bool(selected_raw)
        checked = AccessibilityTools._coerce_bool(checked_raw)
        toggle_state = AccessibilityTools._normalize_toggle_state(toggle_state_raw)
        if checked is None and toggle_state in {"on", "checked"}:
            checked = True
        elif checked is None and toggle_state in {"off", "unchecked"}:
            checked = False
        expanded = AccessibilityTools._coerce_expand_state(expanded_raw)
        value_text = AccessibilityTools._normalize_text_value(value_raw)
        range_value = AccessibilityTools._coerce_number(range_value_raw)
        range_min = AccessibilityTools._coerce_number(range_min_raw)
        range_max = AccessibilityTools._coerce_number(range_max_raw)
        state_tokens: List[str] = []
        if selected is True:
            state_tokens.append("selected")
        elif selected is False:
            state_tokens.append("not selected")
        if checked is True:
            state_tokens.append("checked")
        elif checked is False:
            state_tokens.append("unchecked")
        if toggle_state:
            state_tokens.append(toggle_state)
        if expanded is True:
            state_tokens.append("expanded")
        elif expanded is False:
            state_tokens.append("collapsed")
        if value_text:
            state_tokens.extend(["value", value_text])
        elif range_value is not None:
            state_tokens.extend(["value", str(range_value)])
        state_text = " ".join(token for token in state_tokens if token)
        if not window_title:
            window_title = root_window_title or name
        if not root_window_title:
            root_window_title = window_title or name
        root_handle_value = root_handle if root_handle is not None else handle

        raw_key = f"{handle}|{name}|{control_type}|{auto_id}|{class_name}|{left}|{top}|{width}|{height}"
        digest = hashlib.sha1(raw_key.encode("utf-8", errors="ignore")).hexdigest()[:16]
        element_id = f"uia_{digest}"

        payload = {
            "element_id": element_id,
            "parent_id": parent_id,
            "name": name,
            "window_title": window_title,
            "root_window_title": root_window_title,
            "control_type": control_type,
            "class_name": class_name,
            "automation_id": auto_id,
            "handle": handle,
            "root_handle": root_handle_value,
            "enabled": enabled,
            "visible": visible,
            "selected": selected,
            "checked": checked,
            "toggle_state": toggle_state,
            "expanded": expanded,
            "value_text": value_text,
            "state_text": state_text,
            "range_value": range_value,
            "range_min": range_min,
            "range_max": range_max,
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
    def _safe_call(target: Any, attr_name: str) -> Any:
        if target is None or not attr_name:
            return None
        try:
            attr = getattr(target, attr_name)
        except Exception:
            return None
        if callable(attr):
            try:
                return attr()
            except Exception:
                return None
        return attr

    @staticmethod
    def _safe_path(target: Any, *parts: str) -> Any:
        current = target
        for part in parts:
            if current is None or not part:
                return None
            try:
                current = getattr(current, part)
            except Exception:
                return None
        return current

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
        return None

    @staticmethod
    def _normalize_text_value(value: Any) -> str:
        if value is None:
            return ""
        numeric = AccessibilityTools._coerce_number(value)
        if numeric is not None:
            return str(numeric)
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    @staticmethod
    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if int(value) in {0, 1}:
                return bool(int(value))
            return None
        clean = " ".join(str(value or "").strip().lower().split())
        if clean in {"true", "yes", "on", "checked", "selected", "expanded", "open"}:
            return True
        if clean in {"false", "no", "off", "unchecked", "unselected", "collapsed", "closed"}:
            return False
        return None

    @staticmethod
    def _coerce_expand_state(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            state = int(value)
            if state in {1, 2}:
                return True
            if state in {0, 3}:
                return False
            return None
        clean = " ".join(str(value or "").strip().lower().split())
        if clean in {"expanded", "partially expanded", "open", "opened"}:
            return True
        if clean in {"collapsed", "closed", "leaf", "leaf node"}:
            return False
        return AccessibilityTools._coerce_bool(value)

    @staticmethod
    def _coerce_number(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else round(value, 6)
        clean = str(value).strip().rstrip("%")
        if not clean:
            return None
        try:
            number = float(clean)
        except Exception:
            return None
        return int(number) if number.is_integer() else round(number, 6)

    @staticmethod
    def _normalize_toggle_state(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "on" if value else "off"
        if isinstance(value, (int, float)):
            mapping = {0: "off", 1: "on", 2: "indeterminate"}
            return mapping.get(int(value), str(int(value)))
        clean = " ".join(str(value or "").strip().lower().split())
        mapping = {
            "0": "off",
            "1": "on",
            "2": "indeterminate",
            "true": "on",
            "false": "off",
            "checked": "checked",
            "unchecked": "unchecked",
        }
        return mapping.get(clean, clean)

    @staticmethod
    def _search_text(row: Dict[str, Any]) -> str:
        parts: List[str] = []
        for field in (
            "name",
            "automation_id",
            "class_name",
            "window_title",
            "root_window_title",
            "control_type",
            "state_text",
            "value_text",
        ):
            value = str(row.get(field, "") or "").strip()
            if value:
                parts.append(value.replace("_", " ").replace("-", " "))
        if row.get("range_value") is not None:
            parts.append(str(row.get("range_value")))
        return " ".join(" ".join(parts).strip().lower().split())

    @staticmethod
    def _surface_flags_from_rows(
        rows: List[Dict[str, Any]],
        *,
        control_counts: Counter[str],
    ) -> Dict[str, bool]:
        control_types = {key for key, count in control_counts.items() if count > 0}
        names = " ".join(str(row.get("name", "")).strip().lower() for row in rows if isinstance(row, dict))
        button_like_count = int(control_counts.get("button", 0)) + int(control_counts.get("menuitem", 0))
        dialog_token_hits = sum(
            1
            for token in ("ok", "cancel", "apply", "save", "continue", "next", "warning", "error")
            if token in names
        )
        return {
            "dialog_visible": bool({"window", "dialog"} & control_types)
            and dialog_token_hits >= 2
            and button_like_count >= 2,
            "navigation_tree_visible": bool({"tree", "treeitem"} & control_types),
            "list_surface_visible": bool({"list", "listitem"} & control_types),
            "data_table_visible": bool({"table", "datagrid", "dataitem", "row", "header"} & control_types),
            "tab_strip_visible": bool({"tab", "tabitem"} & control_types),
            "toolbar_visible": bool({"toolbar", "tool bar"} & control_types),
            "menu_visible": bool({"menu", "menuitem"} & control_types),
            "form_surface_visible": bool({"edit", "document", "checkbox", "radiobutton", "combobox", "slider", "spinner"} & control_types),
            "text_entry_surface_visible": bool({"edit", "document"} & control_types),
            "selection_surface_visible": bool({"treeitem", "listitem", "tabitem", "dataitem", "checkbox", "radiobutton"} & control_types),
            "value_control_visible": bool({"slider", "spinner", "progressbar"} & control_types),
            "scrollable_surface_visible": bool({"scrollbar"} & control_types),
            "settings_surface_visible": bool(
                {"checkbox", "radiobutton", "slider", "combobox"} & control_types
            ) and bool({"tree", "list", "tab"} & control_types),
            "search_surface_visible": any(token in names for token in ("search", "find", "filter", "command")),
        }

    @staticmethod
    def _surface_role_candidates(
        *,
        flags: Dict[str, bool],
        control_counts: Counter[str],
    ) -> List[str]:
        candidates: List[str] = []
        if flags.get("dialog_visible"):
            candidates.append("dialog")
        if flags.get("settings_surface_visible"):
            candidates.append("settings")
        if flags.get("data_table_visible"):
            candidates.append("data_console")
        if flags.get("navigation_tree_visible") and flags.get("list_surface_visible"):
            candidates.append("navigator")
        if flags.get("form_surface_visible") and "dialog" not in candidates:
            candidates.append("form")
        if flags.get("text_entry_surface_visible") and control_counts.get("document", 0) >= 1:
            candidates.append("editor")
        if flags.get("toolbar_visible") and flags.get("list_surface_visible"):
            candidates.append("workspace")
        if not candidates:
            candidates.append("content")
        return candidates

    @staticmethod
    def _surface_recommendations(
        *,
        flags: Dict[str, bool],
        query_candidates: List[Dict[str, Any]],
    ) -> List[str]:
        actions: List[str] = []
        if query_candidates:
            actions.append("select_query_target")
        if flags.get("dialog_visible"):
            actions.extend(["confirm_dialog", "dismiss_dialog"])
        if flags.get("navigation_tree_visible"):
            actions.extend(["focus_navigation_tree", "select_tree_item"])
        if flags.get("list_surface_visible"):
            actions.extend(["focus_list_surface", "select_list_item"])
        if flags.get("data_table_visible"):
            actions.extend(["focus_data_table", "select_table_row"])
        if flags.get("tab_strip_visible"):
            actions.append("select_tab_page")
        if flags.get("toolbar_visible"):
            actions.append("focus_toolbar")
        if flags.get("menu_visible"):
            actions.append("select_context_menu_item")
        if flags.get("form_surface_visible"):
            actions.extend(["focus_form_surface", "set_field_value"])
        if flags.get("value_control_visible"):
            actions.append("set_value_control")
        if flags.get("text_entry_surface_visible"):
            actions.append("focus_input_field")
        if flags.get("search_surface_visible"):
            actions.append("focus_search_box")
        deduped: List[str] = []
        for action in actions:
            if action not in deduped:
                deduped.append(action)
        return deduped

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
