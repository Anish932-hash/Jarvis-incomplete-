from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import winreg
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry


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

        path_target = self._resolve_path_target(requested, profile, candidate_terms, exe_candidates)
        if path_target:
            return path_target

        start_app_target = self._resolve_start_app_target(requested, profile, candidate_terms)
        if start_app_target:
            return start_app_target

        shortcut_target = self._resolve_shortcut_target(requested, profile, candidate_terms)
        if shortcut_target:
            return shortcut_target

        uninstall_target = self._resolve_uninstall_target(requested, profile, candidate_terms, exe_candidates)
        if uninstall_target:
            return uninstall_target

        common_path_target = self._resolve_common_path_target(requested, profile, exe_candidates)
        if common_path_target:
            return common_path_target

        shell_target = self._resolve_shell_uri_target(requested, profile, candidate_terms)
        if shell_target:
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
            return result
        if kind == "shell_uri":
            uri = str(target.get("uri", "") or "").strip()
            if not uri:
                return {"status": "error", "message": "Resolved shell URI target is missing a URI."}
            subprocess.Popen(["explorer.exe", uri])
            result = dict(target)
            result["status"] = "success"
            result["launch_method"] = "shell_uri"
            return result
        return {
            "status": "error",
            "message": f"Unsupported launch target kind: {kind or 'unknown'}",
            "requested_app": str(target.get("requested_app", "") or "").strip(),
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
