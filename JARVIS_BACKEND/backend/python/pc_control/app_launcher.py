from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
import winreg
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry
from backend.python.database.local_store import LocalStore


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows


class AppLauncher:
    """Advanced Windows application launcher with inventory-aware resolution."""

    COMMON_PATHS = [
        r"C:\Program Files",
        r"C:\Program Files (x86)",
        r"C:\Windows\System32",
        r"C:\Windows",
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Microsoft", "WindowsApps"),
    ]
    START_MENU_DIRS = [
        os.path.join(os.environ.get("ProgramData", r"C:\ProgramData"), "Microsoft", "Windows", "Start Menu", "Programs"),
        os.path.join(os.environ.get("APPDATA", ""), "Microsoft", "Windows", "Start Menu", "Programs"),
    ]
    APP_PATHS_KEYS: Tuple[Tuple[int, str], ...] = (
        (winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\App Paths"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\Microsoft\Windows\CurrentVersion\App Paths"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Microsoft\Windows\CurrentVersion\App Paths"),
    )
    UNINSTALL_KEYS: Tuple[Tuple[int, str], ...] = (
        (winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
    )
    POWERSHELL_COMMANDS: Tuple[str, ...] = ("powershell", "pwsh")
    START_APPS_COMMAND = (
        "Get-StartApps | "
        "Select-Object Name, AppID | "
        "ConvertTo-Json -Compress"
    )
    KNOWN_SHELL_URIS = {
        "settings": "ms-settings:",
        "windows settings": "ms-settings:",
        "system settings": "ms-settings:",
        "microsoft store": "ms-windows-store:",
        "store": "ms-windows-store:",
    }

    def __init__(self, *, profile_registry: Optional[DesktopAppProfileRegistry] = None) -> None:
        self._profile_registry = profile_registry or DesktopAppProfileRegistry()
        self._start_apps_cache: Optional[List[Dict[str, str]]] = None
        self._shortcut_cache: Optional[List[Dict[str, str]]] = None
        self._uninstall_cache: Optional[List[Dict[str, str]]] = None
        self._app_paths_cache: Optional[List[Dict[str, str]]] = None
        self._user_assist_cache: Optional[List[Dict[str, Any]]] = None
        self._memory_store = LocalStore("data/app_launcher_memory.json")

    def launch(self, app_name: str) -> Dict[str, Any]:
        """Launch an application by path, alias, installed app name, or Start app entry."""

        requested = str(app_name or "").strip()
        if not requested:
            return {"status": "error", "message": "Application name is required."}
        try:
            target = self.resolve_launch_target(requested)
            if target.get("status") != "success":
                return target
            return self._launch_target(target)
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "message": str(exc),
                "requested_app": requested,
            }

    def resolve_launch_target(self, app_name: str) -> Dict[str, Any]:
        requested = str(app_name or "").strip()
        if not requested:
            return {"status": "error", "message": "Application name is required."}

        if os.path.isfile(requested):
            return self._target_payload(
                kind="path",
                requested_app=requested,
                path=requested,
                resolution="direct_path",
                profile={},
            )

        profile = self._match_profile(requested)
        candidate_terms = self._candidate_terms(requested, profile)
        exe_candidates = self._candidate_exe_names(requested, profile, candidate_terms)
        memory_target = self._resolve_memory_target(requested, profile, candidate_terms)
        if memory_target:
            return memory_target

        path_target = self._resolve_path_target(requested, profile, candidate_terms, exe_candidates)
        if path_target:
            self._remember_target(requested=requested, profile=profile, target=path_target, candidate_terms=candidate_terms)
            return path_target

        start_app_target = self._resolve_start_app_target(requested, profile, candidate_terms)
        if start_app_target:
            self._remember_target(requested=requested, profile=profile, target=start_app_target, candidate_terms=candidate_terms)
            return start_app_target

        shortcut_target = self._resolve_shortcut_target(requested, profile, candidate_terms)
        if shortcut_target:
            self._remember_target(requested=requested, profile=profile, target=shortcut_target, candidate_terms=candidate_terms)
            return shortcut_target

        uninstall_target = self._resolve_uninstall_target(requested, profile, candidate_terms, exe_candidates)
        if uninstall_target:
            self._remember_target(requested=requested, profile=profile, target=uninstall_target, candidate_terms=candidate_terms)
            return uninstall_target

        common_path_target = self._resolve_common_path_target(requested, profile, exe_candidates)
        if common_path_target:
            self._remember_target(requested=requested, profile=profile, target=common_path_target, candidate_terms=candidate_terms)
            return common_path_target

        shell_target = self._resolve_shell_uri_target(requested, profile, candidate_terms)
        if shell_target:
            self._remember_target(requested=requested, profile=profile, target=shell_target, candidate_terms=candidate_terms)
            return shell_target

        return {
            "status": "error",
            "message": "Application not found",
            "requested_app": requested,
            "profile": self._profile_summary(profile),
            "candidate_terms": candidate_terms[:20],
            "exe_candidates": exe_candidates[:20],
        }

    def resolve_app_path(self, name: str) -> Optional[str]:
        target = self.resolve_launch_target(name)
        if target.get("status") != "success":
            return None
        if target.get("kind") in {"path", "shortcut"}:
            return str(target.get("path", "") or "").strip() or None
        return None

    def launch_uwp(self, app_name: str) -> Optional[Dict[str, Any]]:
        target = self._resolve_start_app_target(str(app_name or "").strip(), {}, [str(app_name or "").strip()])
        if target:
            return self._launch_target(target)
        target = self._resolve_shell_uri_target(str(app_name or "").strip(), {}, [str(app_name or "").strip()])
        if target:
            return self._launch_target(target)
        return None

    def _launch_target(self, target: Dict[str, Any]) -> Dict[str, Any]:
        kind = str(target.get("kind", "") or "").strip().lower()
        if kind in {"path", "shortcut"}:
            path = str(target.get("path", "") or "").strip()
            if not path:
                return {"status": "error", "message": "Resolved launch target is missing a path."}
            self._start_file(path)
            result = dict(target)
            result["status"] = "success"
            result["launch_method"] = str(target.get("resolution", kind) or kind)
            self._remember_launched_target(result)
            return result
        if kind == "apps_folder":
            app_id = str(target.get("app_id", "") or "").strip()
            if not app_id:
                return {"status": "error", "message": "Resolved Start app target is missing an AppUserModelID."}
            shell_target = rf"shell:AppsFolder\{app_id}"
            subprocess.Popen(["explorer.exe", shell_target])
            result = dict(target)
            result["status"] = "success"
            result["launch_method"] = "apps_folder"
            result["shell_target"] = shell_target
            self._remember_launched_target(result)
            return result
        if kind == "shell_uri":
            uri = str(target.get("uri", "") or "").strip()
            if not uri:
                return {"status": "error", "message": "Resolved shell URI target is missing a URI."}
            subprocess.Popen(["explorer.exe", uri])
            result = dict(target)
            result["status"] = "success"
            result["launch_method"] = "shell_uri"
            self._remember_launched_target(result)
            return result
        return {
            "status": "error",
            "message": f"Unsupported launch target kind: {kind or 'unknown'}",
            "requested_app": str(target.get("requested_app", "") or "").strip(),
        }

    def launch_memory_snapshot(self, *, limit: int = 200, query: str = "", category: str = "") -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 200), 2000))
        clean_query = self._normalize_text(query)
        clean_category = str(category or "").strip().lower()
        rows = list(self._launch_memory_rows())
        if clean_query:
            rows = [
                row
                for row in rows
                if clean_query in self._normalize_text(str(row.get("display_name", "") or ""))
                or clean_query in self._normalize_text(str(row.get("requested_app", "") or ""))
                or clean_query in self._normalize_text(str(row.get("path", "") or ""))
            ]
        if clean_category:
            rows = [
                row
                for row in rows
                if str(row.get("category", "") or "").strip().lower() == clean_category
            ]
        rows.sort(
            key=lambda row: (
                -int(row.get("launch_count", 0) or 0),
                -int(row.get("resolve_count", 0) or 0),
                str(row.get("display_name", "") or row.get("requested_app", "")).strip().lower(),
            )
        )
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "limit": bounded,
            "items": rows[:bounded],
        }

    def invalidate_catalog_cache(self) -> Dict[str, Any]:
        self._start_apps_cache = None
        self._shortcut_cache = None
        self._uninstall_cache = None
        self._app_paths_cache = None
        self._user_assist_cache = None
        return {
            "status": "success",
            "message": "App launcher catalog caches cleared.",
            "cleared": [
                "start_apps",
                "start_menu_shortcuts",
                "uninstall_entries",
                "app_paths",
                "user_assist",
            ],
        }

    def inventory_snapshot(self, *, limit: int = 320, query: str = "", category: str = "") -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 320), 5000))
        clean_query = self._normalize_text(query)
        clean_category = str(category or "").strip().lower()
        usage_rows = self._usage_summary()
        usage_by_name = {
            self._normalize_text(str(row.get("name", "") or "")): dict(row)
            for row in usage_rows
            if self._normalize_text(str(row.get("name", "") or ""))
        }
        usage_by_path = {
            self._normalize_text(str(row.get("path", "") or "")): dict(row)
            for row in usage_rows
            if self._normalize_text(str(row.get("path", "") or ""))
        }
        merged: Dict[str, Dict[str, Any]] = {}

        def _merge_row(row: Dict[str, Any]) -> None:
            dedupe_key = self._inventory_key(row)
            entry = merged.get(dedupe_key, {})
            combined = dict(entry)
            combined.update(
                {
                    key: value
                    for key, value in row.items()
                    if value not in ("", None) and value != []
                }
            )
            combined.setdefault("sources", [])
            source_name = str(row.get("source", "") or row.get("resolution", "") or "unknown").strip().lower() or "unknown"
            if source_name and source_name not in combined["sources"]:
                combined["sources"].append(source_name)
            name_key = self._normalize_text(str(combined.get("display_name", "") or combined.get("name", "") or ""))
            path_key = self._normalize_text(str(combined.get("path", "") or ""))
            usage = usage_by_name.get(name_key, {}) or usage_by_path.get(path_key, {})
            if usage:
                combined["usage"] = usage
                combined["usage_score"] = float(usage.get("usage_score", 0.0) or 0.0)
            else:
                combined.setdefault("usage", {})
                combined.setdefault("usage_score", 0.0)
            profile = self._match_profile(
                str(combined.get("display_name", "") or combined.get("name", "") or combined.get("requested_app", "") or "")
            )
            summary = self._profile_summary(profile)
            if summary and any(summary.values()):
                combined["profile"] = summary
                combined["category"] = str(summary.get("category", "") or combined.get("category", "")).strip()
                combined["canonical_name"] = str(summary.get("canonical_name", "") or combined.get("canonical_name", "")).strip()
            combined["path_ready"] = self._inventory_path_ready(combined)
            combined["kind"] = str(combined.get("kind", "") or "unknown").strip().lower() or "unknown"
            combined["display_name"] = (
                str(combined.get("display_name", "") or combined.get("name", "") or combined.get("requested_app", "")).strip()
            )
            merged[dedupe_key] = combined

        for row in self._load_app_paths_entries():
            _merge_row(dict(row))
        for row in self._load_start_apps():
            _merge_row(
                {
                    "name": str(row.get("name", "") or "").strip(),
                    "display_name": str(row.get("name", "") or "").strip(),
                    "app_id": str(row.get("app_id", "") or "").strip(),
                    "kind": "apps_folder",
                    "resolution": "start_apps",
                    "source": "start_apps",
                }
            )
        for row in self._load_start_menu_shortcuts():
            _merge_row(
                {
                    "name": str(row.get("name", "") or "").strip(),
                    "display_name": str(row.get("name", "") or "").strip(),
                    "path": str(row.get("path", "") or "").strip(),
                    "kind": "shortcut",
                    "resolution": "start_menu_shortcut",
                    "source": "start_menu_shortcut",
                }
            )
        for row in self._load_uninstall_entries():
            _merge_row(
                {
                    "name": str(row.get("name", "") or "").strip(),
                    "display_name": str(row.get("name", "") or "").strip(),
                    "path": str(row.get("path", "") or "").strip(),
                    "install_location": str(row.get("install_location", "") or "").strip(),
                    "kind": "path" if str(row.get("path", "") or "").strip() else "install_location",
                    "resolution": "uninstall_registry",
                    "source": "uninstall_registry",
                }
            )
        for row in self._launch_memory_rows():
            memory_row = dict(row)
            memory_row["source"] = "launch_memory"
            memory_row["resolution"] = str(memory_row.get("resolution", "") or "launch_memory").strip() or "launch_memory"
            _merge_row(memory_row)

        rows = list(merged.values())
        if clean_query:
            rows = [
                row
                for row in rows
                if clean_query in self._normalize_text(str(row.get("display_name", "") or ""))
                or clean_query in self._normalize_text(str(row.get("canonical_name", "") or ""))
                or clean_query in self._normalize_text(str(row.get("path", "") or ""))
            ]
        if clean_category:
            rows = [
                row
                for row in rows
                if str(row.get("category", "") or "").strip().lower() == clean_category
            ]
        rows.sort(
            key=lambda row: (
                0 if bool(row.get("path_ready", False)) else 1,
                -float(row.get("usage_score", 0.0) or 0.0),
                str(row.get("display_name", "") or "").strip().lower(),
            )
        )
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "limit": bounded,
            "path_ready_count": sum(1 for row in rows if bool(row.get("path_ready", False))),
            "frequent_count": sum(1 for row in rows if float(row.get("usage_score", 0.0) or 0.0) >= 1.0),
            "running_count": sum(
                1
                for row in rows
                if bool((row.get("usage", {}) if isinstance(row.get("usage", {}), dict) else {}).get("running", False))
            ),
            "items": rows[:bounded],
        }

    def _start_file(self, path: str) -> None:
        if hasattr(os, "startfile"):
            os.startfile(path)  # type: ignore[attr-defined]
            return
        subprocess.Popen([path], shell=True)

    def _resolve_path_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
        exe_candidates: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        for candidate in exe_candidates:
            resolved = self._query_app_path(candidate)
            if resolved:
                return self._target_payload(
                    kind="path",
                    requested_app=requested,
                    path=resolved,
                    resolution="app_paths_registry",
                    profile=profile,
                    matched_term=candidate,
                )
        for candidate in [*exe_candidates, *candidate_terms]:
            found = shutil.which(str(candidate or "").strip())
            if found:
                return self._target_payload(
                    kind="path",
                    requested_app=requested,
                    path=found,
                    resolution="system_path",
                    profile=profile,
                    matched_term=str(candidate or "").strip(),
                )
        return None

    def _resolve_start_app_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        best_entry: Dict[str, str] = {}
        best_score = 0.0
        best_term = ""
        for entry in self._load_start_apps():
            score, term = self._score_named_entry(
                candidate_terms,
                str(entry.get("name", "") or "").strip(),
                str(entry.get("app_id", "") or "").strip(),
            )
            if score <= best_score:
                continue
            best_score = score
            best_term = term
            best_entry = dict(entry)
        if not best_entry or best_score < 0.84:
            return None
        return self._target_payload(
            kind="apps_folder",
            requested_app=requested,
            app_id=str(best_entry.get("app_id", "") or "").strip(),
            display_name=str(best_entry.get("name", "") or "").strip(),
            resolution="start_apps",
            profile=profile,
            matched_term=best_term,
            match_score=round(best_score, 6),
        )

    def _resolve_shortcut_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        best_entry: Dict[str, str] = {}
        best_score = 0.0
        best_term = ""
        for entry in self._load_start_menu_shortcuts():
            score, term = self._score_named_entry(
                candidate_terms,
                str(entry.get("name", "") or "").strip(),
                str(entry.get("path", "") or "").strip(),
            )
            if score <= best_score:
                continue
            best_score = score
            best_term = term
            best_entry = dict(entry)
        if not best_entry or best_score < 0.84:
            return None
        return self._target_payload(
            kind="shortcut",
            requested_app=requested,
            path=str(best_entry.get("path", "") or "").strip(),
            display_name=str(best_entry.get("name", "") or "").strip(),
            resolution="start_menu_shortcut",
            profile=profile,
            matched_term=best_term,
            match_score=round(best_score, 6),
        )

    def _resolve_uninstall_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
        exe_candidates: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        best_entry: Dict[str, str] = {}
        best_score = 0.0
        best_term = ""
        best_path = ""
        exe_set = {self._normalize_text(value) for value in exe_candidates if str(value).strip()}
        for entry in self._load_uninstall_entries():
            score, term = self._score_named_entry(
                candidate_terms,
                str(entry.get("name", "") or "").strip(),
                str(entry.get("path", "") or "").strip(),
            )
            if score <= 0:
                continue
            path = str(entry.get("path", "") or "").strip()
            if not path:
                candidate_dir = str(entry.get("install_location", "") or "").strip()
                if candidate_dir:
                    for exe_name in exe_candidates:
                        candidate_path = Path(candidate_dir) / exe_name
                        if candidate_path.exists():
                            path = str(candidate_path)
                            break
            if not path or not Path(path).exists():
                continue
            path_name = self._normalize_text(Path(path).name)
            if exe_set and path_name in exe_set:
                score += 0.08
            if score <= best_score:
                continue
            best_score = score
            best_term = term
            best_entry = dict(entry)
            best_path = path
        if not best_entry or not best_path or best_score < 0.72:
            return None
        return self._target_payload(
            kind="path",
            requested_app=requested,
            path=best_path,
            display_name=str(best_entry.get("name", "") or "").strip(),
            resolution="uninstall_registry",
            profile=profile,
            matched_term=best_term,
            match_score=round(best_score, 6),
        )

    def _resolve_common_path_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        exe_candidates: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        for exe_name in exe_candidates:
            resolved = self._search_common_paths(exe_name)
            if not resolved:
                continue
            return self._target_payload(
                kind="path",
                requested_app=requested,
                path=resolved,
                resolution="common_path_scan",
                profile=profile,
                matched_term=exe_name,
            )
        return None

    def _resolve_shell_uri_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        for term in candidate_terms:
            normalized = self._normalize_text(term)
            if normalized not in self.KNOWN_SHELL_URIS:
                continue
            return self._target_payload(
                kind="shell_uri",
                requested_app=requested,
                uri=self.KNOWN_SHELL_URIS[normalized],
                resolution="known_shell_uri",
                profile=profile,
                matched_term=term,
            )
        return None

    def _match_profile(self, app_name: str) -> Dict[str, Any]:
        try:
            match = self._profile_registry.match(app_name=app_name, exe_name=Path(app_name).name)
        except Exception:  # noqa: BLE001
            return {}
        return dict(match) if isinstance(match, dict) and str(match.get("status", "")).strip().lower() == "success" else {}

    def _candidate_terms(self, requested: str, profile: Dict[str, Any]) -> List[str]:
        seed = str(requested or "").strip()
        values: List[str] = [seed]
        values.extend(str(value).strip() for value in profile.get("aliases", []) if str(value).strip())
        values.extend(str(value).strip() for value in profile.get("package_ids", []) if str(value).strip())
        values.extend(str(value).strip() for value in profile.get("window_title_hints", []) if str(value).strip())
        if seed.endswith(".exe"):
            values.append(seed[:-4])
        base_name = Path(seed).stem if any(token in seed for token in (os.sep, "/")) else ""
        if base_name:
            values.append(base_name)
        return _dedupe_strings(values)

    def _candidate_exe_names(self, requested: str, profile: Dict[str, Any], candidate_terms: Sequence[str]) -> List[str]:
        values: List[str] = [str(value).strip() for value in profile.get("exe_hints", []) if str(value).strip()]
        for term in candidate_terms:
            term_text = str(term or "").strip()
            if not term_text:
                continue
            if term_text.lower().endswith(".exe"):
                values.append(term_text)
                continue
            slug = self._slug(term_text)
            if slug:
                values.append(f"{slug}.exe")
            parts = [part for part in re.split(r"[\s._\\/-]+", term_text) if part.strip()]
            if len(parts) >= 2:
                values.append(f"{self._slug(parts[-1])}.exe")
        return _dedupe_strings(values)

    def _load_start_apps(self) -> List[Dict[str, str]]:
        if self._start_apps_cache is not None:
            return list(self._start_apps_cache)
        rows: List[Dict[str, str]] = []
        for executable in self.POWERSHELL_COMMANDS:
            try:
                completed = subprocess.run(
                    [executable, "-NoProfile", "-Command", self.START_APPS_COMMAND],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=20,
                    check=False,
                )
            except Exception:  # noqa: BLE001
                continue
            stdout = str(completed.stdout or "").strip()
            if not stdout:
                continue
            try:
                payload = json.loads(stdout)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                payload = [payload]
            if isinstance(payload, list):
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("Name", item.get("name", "")) or "").strip()
                    app_id = str(item.get("AppID", item.get("AppId", item.get("app_id", ""))) or "").strip()
                    if not (name or app_id):
                        continue
                    rows.append(
                        {
                            "name": name,
                            "app_id": app_id,
                        }
                    )
                if rows:
                    break
        self._start_apps_cache = [
            {
                "name": str(row.get("name", "") or "").strip(),
                "app_id": str(row.get("app_id", "") or "").strip(),
            }
            for row in rows
            if str(row.get("name", "") or "").strip() or str(row.get("app_id", "") or "").strip()
        ]
        return list(self._start_apps_cache)

    def _load_app_paths_entries(self) -> List[Dict[str, str]]:
        if self._app_paths_cache is not None:
            return list(self._app_paths_cache)
        rows: List[Dict[str, str]] = []
        seen: set[str] = set()
        for root, base_key in self.APP_PATHS_KEYS:
            try:
                with winreg.OpenKey(root, base_key) as key:
                    subkey_count = winreg.QueryInfoKey(key)[0]
                    for index in range(subkey_count):
                        subkey_name = winreg.EnumKey(key, index)
                        clean_name = str(subkey_name or "").strip()
                        if not clean_name:
                            continue
                        resolved = self._query_app_path(clean_name)
                        if not resolved:
                            continue
                        normalized_path = self._normalize_text(resolved)
                        if normalized_path in seen:
                            continue
                        seen.add(normalized_path)
                        rows.append(
                            {
                                "name": Path(clean_name).stem or clean_name,
                                "display_name": Path(clean_name).stem or clean_name,
                                "path": resolved,
                                "kind": "path",
                                "resolution": "app_paths_registry",
                                "source": "app_paths_registry",
                            }
                        )
            except OSError:
                continue
        self._app_paths_cache = rows
        return list(self._app_paths_cache)

    def _load_start_menu_shortcuts(self) -> List[Dict[str, str]]:
        if self._shortcut_cache is not None:
            return list(self._shortcut_cache)
        rows: List[Dict[str, str]] = []
        for base in self.START_MENU_DIRS:
            path_obj = Path(base)
            if not path_obj.exists():
                continue
            for pattern in ("*.lnk", "*.url", "*.appref-ms"):
                for item in path_obj.rglob(pattern):
                    rows.append({"name": item.stem, "path": str(item)})
        self._shortcut_cache = rows
        return list(self._shortcut_cache)

    def _load_uninstall_entries(self) -> List[Dict[str, str]]:
        if self._uninstall_cache is not None:
            return list(self._uninstall_cache)
        rows: List[Dict[str, str]] = []
        seen: set[Tuple[str, str]] = set()
        for root, base_key in self.UNINSTALL_KEYS:
            try:
                with winreg.OpenKey(root, base_key) as key:
                    subkey_count = winreg.QueryInfoKey(key)[0]
                    for index in range(subkey_count):
                        subkey_name = winreg.EnumKey(key, index)
                        with winreg.OpenKey(key, subkey_name) as subkey:
                            name = self._registry_value(subkey, "DisplayName")
                            if not name:
                                continue
                            display_icon = self._clean_registry_path(self._registry_value(subkey, "DisplayIcon"))
                            install_location = self._clean_registry_path(self._registry_value(subkey, "InstallLocation"))
                            dedupe_key = (self._normalize_text(name), self._normalize_text(display_icon or install_location))
                            if dedupe_key in seen:
                                continue
                            seen.add(dedupe_key)
                            rows.append(
                                {
                                    "name": name,
                                    "path": display_icon if display_icon and Path(display_icon).exists() else "",
                                    "install_location": install_location,
                                }
                            )
            except OSError:
                continue
        self._uninstall_cache = rows
        return list(self._uninstall_cache)

    def _launch_memory_rows(self) -> List[Dict[str, Any]]:
        payload = self._memory_store.get("targets", {})
        rows = payload if isinstance(payload, dict) else {}
        deduped: Dict[str, Dict[str, Any]] = {}
        for row in rows.values():
            if not isinstance(row, dict):
                continue
            item = dict(row)
            key = self._inventory_key(item)
            current = deduped.get(key)
            if current is None or int(item.get("launch_count", 0) or 0) >= int(current.get("launch_count", 0) or 0):
                deduped[key] = item
        return list(deduped.values())

    def _remember_target(
        self,
        *,
        requested: str,
        profile: Dict[str, Any],
        target: Dict[str, Any],
        candidate_terms: Sequence[str],
    ) -> None:
        if not isinstance(target, dict) or str(target.get("status", "success") or "").strip().lower() != "success":
            return
        if not self._target_is_valid(target):
            return
        rows = self._memory_store.get("targets", {})
        store_rows = dict(rows) if isinstance(rows, dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()
        aliases = _dedupe_strings(
            [
                requested,
                str(target.get("requested_app", "") or "").strip(),
                str(target.get("display_name", "") or "").strip(),
                str(target.get("matched_term", "") or "").strip(),
                str(profile.get("name", "") or "").strip(),
                str(profile.get("canonical_name", "") or "").strip(),
                *[str(item).strip() for item in candidate_terms if str(item).strip()],
                *[str(item).strip() for item in profile.get("aliases", []) if str(item).strip()],
            ]
        )
        base_row = {
            "kind": str(target.get("kind", "") or "").strip().lower(),
            "requested_app": str(requested or "").strip(),
            "display_name": str(target.get("display_name", "") or target.get("requested_app", "") or requested).strip(),
            "path": str(target.get("path", "") or "").strip(),
            "app_id": str(target.get("app_id", "") or "").strip(),
            "uri": str(target.get("uri", "") or "").strip(),
            "resolution": str(target.get("resolution", "") or "").strip(),
            "category": str(profile.get("category", "") or target.get("category", "") or "").strip(),
            "canonical_name": str(profile.get("canonical_name", "") or "").strip(),
            "profile_name": str(profile.get("name", "") or "").strip(),
            "aliases": aliases[:24],
            "resolved_at": now_iso,
        }
        for alias in aliases:
            normalized = self._normalize_text(alias)
            if not normalized:
                continue
            current = store_rows.get(normalized, {})
            current_row = dict(current) if isinstance(current, dict) else {}
            updated = dict(current_row)
            updated.update(
                {
                    key: value
                    for key, value in base_row.items()
                    if value not in ("", None) and value != []
                }
            )
            updated["resolve_count"] = int(updated.get("resolve_count", 0) or 0) + 1
            updated.setdefault("launch_count", int(current_row.get("launch_count", 0) or 0))
            updated["last_resolved_at"] = now_iso
            store_rows[normalized] = updated
        self._memory_store.set("targets", store_rows)

    def _remember_launched_target(self, target: Dict[str, Any]) -> None:
        if not isinstance(target, dict):
            return
        requested = str(target.get("requested_app", "") or target.get("display_name", "") or "").strip()
        aliases = [
            requested,
            str(target.get("display_name", "") or "").strip(),
        ]
        rows = self._memory_store.get("targets", {})
        store_rows = dict(rows) if isinstance(rows, dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()
        for alias in aliases:
            normalized = self._normalize_text(alias)
            if not normalized:
                continue
            current = store_rows.get(normalized, {})
            if not isinstance(current, dict):
                continue
            current_row = dict(current)
            current_row["launch_count"] = int(current_row.get("launch_count", 0) or 0) + 1
            current_row["last_launched_at"] = now_iso
            store_rows[normalized] = current_row
        if store_rows:
            self._memory_store.set("targets", store_rows)

    def _resolve_memory_target(
        self,
        requested: str,
        profile: Dict[str, Any],
        candidate_terms: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        rows = self._memory_store.get("targets", {})
        if not isinstance(rows, dict) or not rows:
            return None
        lookup_terms = _dedupe_strings(
            [
                requested,
                str(profile.get("name", "") or "").strip(),
                str(profile.get("canonical_name", "") or "").strip(),
                *[str(item).strip() for item in candidate_terms if str(item).strip()],
                *[str(item).strip() for item in profile.get("aliases", []) if str(item).strip()],
            ]
        )
        for term in lookup_terms:
            normalized = self._normalize_text(term)
            if not normalized:
                continue
            row = rows.get(normalized)
            if not isinstance(row, dict):
                continue
            if not self._target_is_valid(row):
                continue
            payload = dict(row)
            payload["status"] = "success"
            payload["requested_app"] = requested
            payload["resolution"] = "launch_memory"
            payload["memory_hit"] = True
            payload["memory_confidence"] = "remembered_exact"
            payload["profile"] = self._profile_summary(profile)
            return payload
        return None

    def _usage_summary(self) -> List[Dict[str, Any]]:
        launch_rows = self._launch_memory_rows()
        user_assist_rows = self._load_user_assist_usage()
        running_rows = self._load_running_processes()
        usage: Dict[str, Dict[str, Any]] = {}

        def _merge(name: str, *, path: str = "", launch_count: int = 0, resolve_count: int = 0, run_count: int = 0, running: bool = False, last_seen_at: str = "") -> None:
            normalized = self._normalize_text(name or path)
            if not normalized:
                return
            entry = usage.get(normalized, {"name": str(name or "").strip(), "path": str(path or "").strip()})
            entry["name"] = str(entry.get("name", "") or name or "").strip()
            entry["path"] = str(entry.get("path", "") or path or "").strip()
            entry["launch_count"] = int(entry.get("launch_count", 0) or 0) + max(0, int(launch_count or 0))
            entry["resolve_count"] = int(entry.get("resolve_count", 0) or 0) + max(0, int(resolve_count or 0))
            entry["run_count"] = int(entry.get("run_count", 0) or 0) + max(0, int(run_count or 0))
            entry["running"] = bool(entry.get("running", False) or running)
            if last_seen_at:
                existing_last = str(entry.get("last_seen_at", "") or "").strip()
                if not existing_last or str(last_seen_at) > existing_last:
                    entry["last_seen_at"] = str(last_seen_at)
            usage[normalized] = entry

        for row in launch_rows:
            if not isinstance(row, dict):
                continue
            _merge(
                str(row.get("display_name", "") or row.get("requested_app", "") or "").strip(),
                path=str(row.get("path", "") or "").strip(),
                launch_count=int(row.get("launch_count", 0) or 0),
                resolve_count=int(row.get("resolve_count", 0) or 0),
                last_seen_at=str(row.get("last_launched_at", "") or row.get("last_resolved_at", "") or "").strip(),
            )
        for row in user_assist_rows:
            if not isinstance(row, dict):
                continue
            _merge(
                str(row.get("name", "") or "").strip(),
                path=str(row.get("path", "") or "").strip(),
                run_count=int(row.get("run_count", 0) or 0),
                last_seen_at=str(row.get("last_seen_at", "") or "").strip(),
            )
        for row in running_rows:
            if not isinstance(row, dict):
                continue
            _merge(
                str(row.get("name", "") or "").strip(),
                path=str(row.get("path", "") or "").strip(),
                running=True,
            )

        rows: List[Dict[str, Any]] = []
        for row in usage.values():
            item = dict(row)
            item["usage_score"] = round(
                float(item.get("launch_count", 0) or 0) * 3.0
                + float(item.get("run_count", 0) or 0)
                + (5.0 if bool(item.get("running", False)) else 0.0),
                3,
            )
            rows.append(item)
        rows.sort(key=lambda row: (-float(row.get("usage_score", 0.0) or 0.0), str(row.get("name", "") or "").strip().lower()))
        return rows

    def _load_running_processes(self) -> List[Dict[str, Any]]:
        try:
            import psutil  # type: ignore
        except Exception:
            return []
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for process in psutil.process_iter(["name", "exe"]):
            try:
                name = str(process.info.get("name", "") or "").strip()
                path = str(process.info.get("exe", "") or "").strip()
            except Exception:
                continue
            if not name and not path:
                continue
            key = self._normalize_text(path or name)
            if not key or key in seen:
                continue
            seen.add(key)
            rows.append({"name": Path(path).stem if path else Path(name).stem, "path": path})
        return rows

    def _load_user_assist_usage(self) -> List[Dict[str, Any]]:
        if self._user_assist_cache is not None:
            return list(self._user_assist_cache)
        rows: List[Dict[str, Any]] = []
        base_key = r"Software\Microsoft\Windows\CurrentVersion\Explorer\UserAssist"
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, base_key) as root:
                guid_count = winreg.QueryInfoKey(root)[0]
                for index in range(guid_count):
                    guid_name = winreg.EnumKey(root, index)
                    count_key_path = rf"{base_key}\{guid_name}\Count"
                    try:
                        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, count_key_path) as count_key:
                            value_count = winreg.QueryInfoKey(count_key)[1]
                            for value_index in range(value_count):
                                encoded_name, raw_value, _ = winreg.EnumValue(count_key, value_index)
                                clean_name = self._decode_rot13(encoded_name)
                                if not clean_name:
                                    continue
                                parsed = self._parse_user_assist_value(raw_value)
                                path_text = clean_name if clean_name.lower().endswith(".exe") or "\\" in clean_name else ""
                                display_name = Path(clean_name).stem if path_text else clean_name.split("\\")[-1]
                                rows.append(
                                    {
                                        "name": display_name,
                                        "path": path_text,
                                        "run_count": int(parsed.get("run_count", 0) or 0),
                                        "last_seen_at": str(parsed.get("last_seen_at", "") or "").strip(),
                                    }
                                )
                    except OSError:
                        continue
        except OSError:
            rows = []
        deduped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            key = self._normalize_text(str(row.get("path", "") or row.get("name", "") or ""))
            if not key:
                continue
            current = deduped.get(key, {})
            current_run_count = int(current.get("run_count", 0) or 0)
            row_run_count = int(row.get("run_count", 0) or 0)
            if not current or row_run_count >= current_run_count:
                deduped[key] = dict(row)
        self._user_assist_cache = list(deduped.values())
        return list(self._user_assist_cache)

    def _query_app_path(self, exe_name: str) -> Optional[str]:
        clean_exe = str(exe_name or "").strip()
        if not clean_exe:
            return None
        if not clean_exe.lower().endswith(".exe"):
            clean_exe = f"{clean_exe}.exe"
        for root, base_key in self.APP_PATHS_KEYS:
            try:
                with winreg.OpenKey(root, rf"{base_key}\{clean_exe}") as key:
                    default_value = self._clean_registry_path(self._registry_value(key, None))
                    if default_value and Path(default_value).exists():
                        return default_value
                    candidate_dir = self._clean_registry_path(self._registry_value(key, "Path"))
                    if candidate_dir:
                        candidate = Path(candidate_dir) / clean_exe
                        if candidate.exists():
                            return str(candidate)
            except OSError:
                continue
        return None

    def _search_common_paths(self, exe_name: str) -> Optional[str]:
        clean_exe = str(exe_name or "").strip()
        if not clean_exe:
            return None
        names = {clean_exe}
        if not clean_exe.lower().endswith(".exe"):
            names.add(f"{clean_exe}.exe")
        for base in self.COMMON_PATHS:
            path_obj = Path(base)
            if not path_obj.exists():
                continue
            try:
                for root, _, files in os.walk(path_obj):
                    file_set = {str(file_name).lower(): file_name for file_name in files}
                    for candidate in names:
                        file_name = file_set.get(candidate.lower())
                        if file_name:
                            return str(Path(root) / file_name)
            except OSError:
                continue
        return None

    @staticmethod
    def _score_named_entry(candidate_terms: Sequence[str], primary_name: str, alternate_name: str = "") -> Tuple[float, str]:
        normalized_primary = AppLauncher._normalize_text(primary_name)
        normalized_alternate = AppLauncher._normalize_text(alternate_name)
        best_score = 0.0
        best_term = ""
        for term in candidate_terms:
            normalized_term = AppLauncher._normalize_text(term)
            if not normalized_term:
                continue
            score = 0.0
            if normalized_term == normalized_primary or normalized_term == normalized_alternate:
                score = 1.0
            elif normalized_primary and normalized_term in normalized_primary:
                score = 0.94
            elif normalized_term and normalized_primary and normalized_primary in normalized_term:
                score = 0.91
            elif normalized_alternate and normalized_term in normalized_alternate:
                score = 0.89
            elif normalized_term and normalized_alternate and normalized_alternate in normalized_term:
                score = 0.86
            if score > best_score:
                best_score = score
                best_term = str(term or "").strip()
        return best_score, best_term

    @staticmethod
    def _registry_value(key: Any, value_name: Optional[str]) -> str:
        try:
            value, _ = winreg.QueryValueEx(key, value_name)
        except OSError:
            return ""
        return str(value or "").strip()

    @staticmethod
    def _clean_registry_path(raw_value: str) -> str:
        value = os.path.expandvars(str(raw_value or "").strip().strip('"'))
        if not value:
            return ""
        if "," in value:
            value = value.split(",", 1)[0].strip().strip('"')
        if value.lower().endswith(".exe") or value.lower().endswith(".lnk") or value.lower().endswith(".msc"):
            return value
        return value if Path(value).exists() else ""

    @staticmethod
    def _decode_rot13(value: str) -> str:
        try:
            return str(value or "").translate(
                str.maketrans(
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
                    "NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm",
                )
            )
        except Exception:
            return str(value or "")

    @staticmethod
    def _parse_user_assist_value(raw_value: Any) -> Dict[str, Any]:
        if not isinstance(raw_value, (bytes, bytearray)):
            return {"run_count": 0, "last_seen_at": ""}
        data = bytes(raw_value)
        run_count = int.from_bytes(data[4:8], "little", signed=False) if len(data) >= 8 else 0
        last_seen_at = ""
        if len(data) >= 68:
            filetime_value = int.from_bytes(data[60:68], "little", signed=False)
            if filetime_value > 0:
                try:
                    timestamp = (filetime_value / 10_000_000.0) - 11_644_473_600.0
                    if timestamp > 0:
                        last_seen_at = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                except Exception:
                    last_seen_at = ""
        return {"run_count": max(0, run_count), "last_seen_at": last_seen_at}

    def _target_is_valid(self, target: Dict[str, Any]) -> bool:
        kind = str(target.get("kind", "") or "").strip().lower()
        if kind in {"path", "shortcut"}:
            return bool(str(target.get("path", "") or "").strip()) and Path(str(target.get("path", "") or "").strip()).exists()
        if kind == "apps_folder":
            return bool(str(target.get("app_id", "") or "").strip())
        if kind == "shell_uri":
            return bool(str(target.get("uri", "") or "").strip())
        return False

    def _inventory_key(self, row: Dict[str, Any]) -> str:
        path = str(row.get("path", "") or "").strip()
        app_id = str(row.get("app_id", "") or "").strip()
        uri = str(row.get("uri", "") or "").strip()
        name = str(row.get("display_name", "") or row.get("name", "") or row.get("requested_app", "")).strip()
        if path:
            return f"path:{path.lower()}"
        if app_id:
            return f"app:{app_id.lower()}"
        if uri:
            return f"uri:{uri.lower()}"
        return f"name:{self._normalize_text(name)}"

    @staticmethod
    def _inventory_path_ready(row: Dict[str, Any]) -> bool:
        kind = str(row.get("kind", "") or "").strip().lower()
        if kind in {"path", "shortcut"}:
            return bool(str(row.get("path", "") or "").strip()) and Path(str(row.get("path", "") or "").strip()).exists()
        if kind == "install_location":
            return bool(str(row.get("install_location", "") or "").strip()) and Path(str(row.get("install_location", "") or "").strip()).exists()
        if kind == "apps_folder":
            return bool(str(row.get("app_id", "") or "").strip())
        if kind == "shell_uri":
            return bool(str(row.get("uri", "") or "").strip())
        return False

    @staticmethod
    def _target_payload(kind: str, requested_app: str, profile: Dict[str, Any], **extra: Any) -> Dict[str, Any]:
        payload = {
            "status": "success",
            "kind": kind,
            "requested_app": str(requested_app or "").strip(),
            "profile": AppLauncher._profile_summary(profile),
        }
        payload.update(extra)
        return payload

    @staticmethod
    def _profile_summary(profile: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(profile, dict):
            return {}
        return {
            "name": str(profile.get("name", "") or "").strip(),
            "canonical_name": str(profile.get("canonical_name", "") or "").strip(),
            "category": str(profile.get("category", "") or "").strip(),
            "aliases": [str(value).strip() for value in profile.get("aliases", []) if str(value).strip()][:10],
            "exe_hints": [str(value).strip() for value in profile.get("exe_hints", []) if str(value).strip()][:10],
            "package_ids": [str(value).strip() for value in profile.get("package_ids", []) if str(value).strip()][:10],
        }

    @staticmethod
    def _normalize_text(value: str) -> str:
        text = str(value or "").strip().lower()
        text = text.replace("&", " and ")
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _slug(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", AppLauncher._normalize_text(value))
