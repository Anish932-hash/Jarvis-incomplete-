from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


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


def _defaults(
    *,
    target_mode: str,
    verify_mode: str,
    verify_text_source: str,
    capability_preferences: List[str],
    risk_posture: str = "medium",
    max_strategy_attempts: int = 2,
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "routing_defaults": {"target_mode": target_mode, "verify_mode": verify_mode},
        "autonomy_defaults": {
            "ensure_app_launch": True,
            "focus_first": True,
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": max_strategy_attempts,
        },
        "verification_defaults": {
            "prefer_window_match": True,
            "prefer_text_visibility": verify_text_source in {"typed_text", "query_or_typed"},
            "verify_text_source": verify_text_source,
        },
        "capability_preferences": list(capability_preferences),
        "risk_posture": risk_posture,
        "warnings": list(warnings or []),
    }


class DesktopAppProfileRegistry:
    DEFAULT_PATHS = (r"E:\apps.txt", r"C:\apps.txt")
    PACKAGE_ID_STOPWORDS = {
        "app", "application", "apps", "arp", "desktop", "exe", "machine", "msix", "store", "user", "users", "winget", "x64", "x86",
    }
    CATEGORY_DEFAULTS: Dict[str, Dict[str, Any]] = {
        "browser": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="query_or_typed", capability_preferences=["accessibility", "vision"], max_strategy_attempts=3),
        "code_editor": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"]),
        "ide": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"]),
        "terminal": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"]),
        "chat": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"], max_strategy_attempts=3, warnings=["Message and chat clients may contain transient banners, so OCR verification can be noisy."]),
        "office": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"], max_strategy_attempts=3),
        "media": _defaults(target_mode="ocr", verify_mode="hash_changed", verify_text_source="query", capability_preferences=["vision", "accessibility"], max_strategy_attempts=3, warnings=["Media and creative apps often render custom canvases, so OCR fallbacks may be required."]),
        "utility": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="query", capability_preferences=["accessibility", "vision"]),
        "ops_console": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"], risk_posture="high", warnings=["Ops and infrastructure tools can trigger destructive workflows, so verification remains strict."]),
        "security": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="query", capability_preferences=["accessibility", "vision"], risk_posture="high", warnings=["Security and VPN apps may require elevated prompts or protected UI flows."]),
        "remote_support": _defaults(target_mode="ocr", verify_mode="state_or_visibility", verify_text_source="query", capability_preferences=["vision", "accessibility"], risk_posture="high", max_strategy_attempts=3, warnings=["Remote desktop surfaces can duplicate or proxy UI, so OCR-based verification is preferred."]),
        "game": _defaults(target_mode="ocr", verify_mode="hash_changed", verify_text_source="none", capability_preferences=["vision"], risk_posture="high", warnings=["Games often use custom rendering or anti-cheat protections, so direct automation can be flaky."]),
        "ai_companion": _defaults(target_mode="accessibility", verify_mode="state_or_visibility", verify_text_source="typed_text", capability_preferences=["accessibility", "vision"]),
        "general_desktop": _defaults(target_mode="auto", verify_mode="state_or_visibility", verify_text_source="query_or_typed", capability_preferences=["accessibility", "vision"]),
    }
    SPECIAL_OVERRIDES: Dict[str, Dict[str, Any]] = {
        "google chrome": {"aliases": ["chrome", "google chrome"], "exe_hints": ["chrome.exe"], "category": "browser"},
        "microsoft edge": {"aliases": ["edge", "microsoft edge"], "exe_hints": ["msedge.exe"], "category": "browser"},
        "brave": {"aliases": ["brave"], "exe_hints": ["brave.exe"], "category": "browser"},
        "warp": {"aliases": ["warp", "warp terminal"], "exe_hints": ["warp.exe"], "category": "terminal"},
        "cloudflare warp": {"aliases": ["cloudflare warp", "warp vpn"], "exe_hints": ["cloudflarewarp.exe"], "category": "security"},
        "microsoft visual studio code": {"aliases": ["vscode", "visual studio code", "code"], "exe_hints": ["code.exe"], "category": "code_editor"},
        "visual studio community": {"aliases": ["visual studio", "vs"], "exe_hints": ["devenv.exe"], "category": "ide"},
        "pycharm": {"aliases": ["pycharm"], "exe_hints": ["pycharm64.exe"], "category": "ide"},
        "powershell": {"aliases": ["powershell", "pwsh"], "exe_hints": ["pwsh.exe", "powershell.exe"], "category": "terminal"},
        "windows terminal": {"aliases": ["windows terminal", "terminal"], "exe_hints": ["windowsterminal.exe", "wt.exe"], "category": "terminal"},
        "discord": {"aliases": ["discord"], "exe_hints": ["discord.exe"], "category": "chat"},
        "telegram desktop": {"aliases": ["telegram", "telegram desktop"], "exe_hints": ["telegram.exe"], "category": "chat"},
        "whatsapp": {"aliases": ["whatsapp"], "exe_hints": ["whatsapp.exe"], "category": "chat"},
        "microsoft teams": {"aliases": ["teams", "microsoft teams"], "exe_hints": ["ms-teams.exe", "teams.exe"], "category": "chat"},
        "proton mail": {"aliases": ["proton mail"], "exe_hints": ["protonmail.exe"], "category": "office"},
        "outlook for windows": {"aliases": ["outlook"], "exe_hints": ["olk.exe", "outlook.exe"], "category": "office"},
        "docker desktop": {"aliases": ["docker", "docker desktop"], "exe_hints": ["docker.exe"], "category": "ops_console"},
        "vmware workstation": {"aliases": ["vmware", "vmware workstation"], "exe_hints": ["vmware.exe"], "category": "ops_console"},
        "anydesk": {"aliases": ["anydesk"], "exe_hints": ["anydesk.exe"], "category": "remote_support"},
        "chatgpt": {"aliases": ["chatgpt"], "exe_hints": ["chatgpt.exe"], "category": "ai_companion"},
        "claude": {"aliases": ["claude"], "exe_hints": ["claude.exe"], "category": "ai_companion"},
        "codex": {"aliases": ["codex"], "exe_hints": ["codex.exe"], "category": "ai_companion"},
        "copilot": {"aliases": ["copilot", "microsoft copilot"], "exe_hints": ["copilot.exe"], "category": "ai_companion"},
        "ollama": {"aliases": ["ollama"], "exe_hints": ["ollama.exe"], "category": "ai_companion"},
        "roblox player": {"aliases": ["roblox", "roblox player"], "exe_hints": ["robloxplayerbeta.exe"], "category": "game"},
        "roblox studio": {"aliases": ["roblox studio"], "exe_hints": ["robloxstudiobeta.exe"], "category": "game"},
        "x minecraft launcher": {"aliases": ["minecraft", "minecraft launcher"], "exe_hints": ["minecraftlauncher.exe"], "category": "game"},
        "tlauncher": {"aliases": ["tlauncher"], "exe_hints": ["tlauncher.exe"], "category": "game"},
    }

    def __init__(self, *, source_paths: Optional[List[str]] = None) -> None:
        self._source_paths = [str(Path(path).expanduser()) for path in (source_paths or list(self.DEFAULT_PATHS)) if str(path or "").strip()]
        self._profile_list: List[Dict[str, Any]] = []
        self._category_counts: Dict[str, int] = {}
        self._loaded = False

    def catalog(self, *, query: str = "", category: str = "", limit: int = 400) -> Dict[str, Any]:
        self._ensure_loaded()
        clean_query = self._normalize_text(query)
        clean_category = self._normalize_text(category)
        rows = [dict(profile) for profile in self._profile_list]
        if clean_category:
            rows = [profile for profile in rows if self._normalize_text(profile.get("category", "")) == clean_category]
        if clean_query:
            rows = [
                profile
                for profile in rows
                if clean_query in self._normalize_text(profile.get("name", ""))
                or any(clean_query in self._normalize_text(alias) for alias in profile.get("aliases", []) if str(alias).strip())
                or any(clean_query in self._normalize_text(package_id) for package_id in profile.get("package_ids", []) if str(package_id).strip())
            ]
        bounded = max(1, min(int(limit or 400), 2000))
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "category_counts": dict(self._category_counts),
            "items": rows[:bounded],
            "source_paths": list(self._source_paths),
        }

    def match(self, *, app_name: str = "", window_title: str = "", exe_name: str = "") -> Dict[str, Any]:
        self._ensure_loaded()
        clean_app = self._normalize_text(app_name)
        clean_title = self._normalize_text(window_title)
        clean_exe = self._normalize_text(exe_name)
        best: Dict[str, Any] = {}
        best_score = 0.0
        best_reasons: List[str] = []
        for profile in self._profile_list:
            score, reasons = self._score_profile(profile, app_name=clean_app, window_title=clean_title, exe_name=clean_exe)
            if score <= best_score:
                continue
            best_score = score
            best_reasons = reasons
            best = dict(profile)
            best["match_score"] = round(score, 6)
        if not best:
            return {"status": "unmatched", "match_score": 0.0, "match_reasons": []}
        best["status"] = "success"
        best["match_reasons"] = best_reasons
        return best

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        apps: Dict[str, Dict[str, Any]] = {}
        for source_path in self._source_paths:
            path_obj = Path(source_path)
            if not path_obj.exists():
                continue
            try:
                raw_text = path_obj.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for app_row in self._parse_app_rows(raw_text):
                app_name = str(app_row.get("name", "") or "").strip()
                normalized = self._normalize_text(app_name)
                if not normalized:
                    continue
                row = apps.get(normalized, {"name": app_name, "sources": [], "source_paths": [], "package_ids": [], "versions": [], "available_versions": [], "package_sources": []})
                row["name"] = str(row.get("name", app_name) or app_name)
                row["sources"] = _dedupe_strings(list(row.get("sources", [])) + [path_obj.name])
                row["source_paths"] = _dedupe_strings(list(row.get("source_paths", [])) + [str(path_obj)])
                row["package_ids"] = _dedupe_strings(list(row.get("package_ids", [])) + [str(app_row.get("package_id", "") or "")])
                row["versions"] = _dedupe_strings(list(row.get("versions", [])) + [str(app_row.get("version", "") or "")])
                row["available_versions"] = _dedupe_strings(list(row.get("available_versions", [])) + [str(app_row.get("available", "") or "")])
                row["package_sources"] = _dedupe_strings(list(row.get("package_sources", [])) + [str(app_row.get("source", "") or "")])
                apps[normalized] = row
        profiles: List[Dict[str, Any]] = []
        counts: Dict[str, int] = {}
        for normalized_name, app_row in apps.items():
            profile = self._build_profile(app_name=str(app_row.get("name", normalized_name)), app_row=app_row)
            profiles.append(profile)
            category_name = str(profile.get("category", "general_desktop") or "general_desktop").strip().lower()
            counts[category_name] = int(counts.get(category_name, 0)) + 1
        profiles.sort(key=lambda row: (str(row.get("category", "")), str(row.get("name", "")).lower()))
        self._profile_list = profiles
        self._category_counts = counts
        self._loaded = True

    def _build_profile(self, *, app_name: str, app_row: Dict[str, Any]) -> Dict[str, Any]:
        package_ids = [str(value).strip() for value in app_row.get("package_ids", []) if str(value).strip()]
        canonical_name = self._canonical_name(app_name)
        override = self._special_override(canonical_name, app_name, package_ids)
        category = str(override.get("category", self._infer_category(canonical_name, app_name, package_ids)) or "general_desktop").strip().lower()
        defaults = self.CATEGORY_DEFAULTS.get(category, self.CATEGORY_DEFAULTS["general_desktop"])
        aliases = _dedupe_strings(list(override.get("aliases", [])) + [app_name, canonical_name] + self._keyword_aliases(canonical_name, app_name) + self._package_id_aliases(package_ids))
        exe_hints = _dedupe_strings(list(override.get("exe_hints", [])) + self._package_id_exe_hints(package_ids) + self._exe_hints(canonical_name, aliases))
        return {
            "profile_id": self._slug(aliases[0] if aliases else app_name),
            "name": app_name,
            "canonical_name": canonical_name,
            "category": category,
            "risk_posture": str(defaults.get("risk_posture", "medium") or "medium"),
            "aliases": aliases,
            "exe_hints": exe_hints,
            "window_title_hints": aliases[:10],
            "package_ids": package_ids,
            "versions": [str(value).strip() for value in app_row.get("versions", []) if str(value).strip()],
            "available_versions": [str(value).strip() for value in app_row.get("available_versions", []) if str(value).strip()],
            "package_sources": [str(value).strip() for value in app_row.get("package_sources", []) if str(value).strip()],
            "autonomy_defaults": dict(defaults.get("autonomy_defaults", {})),
            "routing_defaults": dict(defaults.get("routing_defaults", {})),
            "verification_defaults": dict(defaults.get("verification_defaults", {})),
            "capability_preferences": list(defaults.get("capability_preferences", [])),
            "warnings": _dedupe_strings(list(defaults.get("warnings", [])) + list(override.get("warnings", []))),
            "installed_sources": list(app_row.get("sources", [])),
            "source_paths": list(app_row.get("source_paths", [])),
        }

    def _special_override(self, canonical_name: str, app_name: str, package_ids: List[str]) -> Dict[str, Any]:
        exact_candidates = {canonical_name, self._normalize_text(app_name), *[self._normalize_text(package_id) for package_id in package_ids]}
        for key, override in self.SPECIAL_OVERRIDES.items():
            if self._normalize_text(key) in exact_candidates:
                return dict(override)
        haystack = " ".join(sorted(value for value in exact_candidates if value))
        for key, override in self.SPECIAL_OVERRIDES.items():
            normalized_key = self._normalize_text(key)
            if normalized_key and normalized_key in haystack:
                return dict(override)
        return {}

    def _infer_category(self, canonical_name: str, app_name: str, package_ids: List[str]) -> str:
        haystack = " ".join(value for value in [canonical_name, self._normalize_text(app_name)] + [self._normalize_text(package_id) for package_id in package_ids] if value)
        if any(keyword in haystack for keyword in ("roblox", "minecraft", "krunker", "launcher", "xbox", "solitaire", "warships", "steam", "tlauncher")):
            return "game"
        if any(keyword in haystack for keyword in ("cloudflare warp", "vpn", "security", "defender", "antivirus", "firewall", "authenticator")):
            return "security"
        if any(keyword in haystack for keyword in ("anydesk", "quick assist", "teamviewer", "remote desktop")):
            return "remote_support"
        if any(keyword in haystack for keyword in ("chrome", "edge", "brave", "browser", "firefox", "opera", "vivaldi")):
            return "browser"
        if any(keyword in haystack for keyword in ("visual studio code", "vscode", "notepad", "notepad++", "sublime", "zed", "cursor")):
            return "code_editor"
        if any(keyword in haystack for keyword in ("pycharm", "visual studio", "intellij", "android studio", "webstorm", "rider", "claude code")):
            return "ide"
        if any(keyword in haystack for keyword in ("powershell", "pwsh", "windows terminal", "terminal", "command prompt", "cmd", "hyper", "tabby")):
            return "terminal"
        if any(keyword in haystack for keyword in ("discord", "telegram", "teams", "whatsapp", "signal", "slack")):
            return "chat"
        if any(keyword in haystack for keyword in ("word", "excel", "powerpoint", "office", "outlook", "mail", "calendar", "onenote", "sticky notes", "to do", "proton mail")):
            return "office"
        if any(keyword in haystack for keyword in ("screen recorder", "loom", "medal", "photos", "paint", "clipchamp", "sound recorder", "media player", "freetube", "youtube", "vlc", "obs", "fxsound", "spotify")):
            return "media"
        if any(keyword in haystack for keyword in ("docker", "vmware", "wsl", "build tools", "sdk", "git", "github desktop", "postman", "insomnia", "virtualbox")):
            return "ops_console"
        if any(keyword in haystack for keyword in ("chatgpt", "claude", "codex", "copilot", "jarvis", "ollama", "hackerai", "wispr", "firebase studio", "jioai", "antigravity")):
            return "ai_companion"
        if any(keyword in haystack for keyword in ("zip", "recuva", "everything", "onedrive", "gopeed", "torrent", "rufus", "terabox", "installer", "dropbox", "drive")):
            return "utility"
        return "general_desktop"

    @staticmethod
    def _parse_app_rows(raw_text: str) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        seen: set[Tuple[str, str]] = set()
        for line in str(raw_text or "").splitlines():
            stripped = line.rstrip()
            if not stripped.strip():
                continue
            clean = stripped.strip()
            if clean in {"-", "\\", "/"} or set(clean) == {"-"}:
                continue
            parts = [part.strip() for part in re.split(r"\s{2,}", stripped) if part.strip()]
            if not parts or parts[0].lower() == "name":
                continue
            if len(parts) == 1 and len(parts[0]) < 3:
                continue
            row = {
                "name": str(parts[0] or "").strip(),
                "package_id": str(parts[1] if len(parts) > 1 else "").strip(),
                "version": str(parts[2] if len(parts) > 2 else "").strip(),
                "available": str(parts[3] if len(parts) > 3 else "").strip(),
                "source": str(parts[4] if len(parts) > 4 else "").strip(),
            }
            if not row["name"]:
                continue
            dedupe_key = (DesktopAppProfileRegistry._normalize_text(row["name"]), DesktopAppProfileRegistry._normalize_text(row["package_id"]))
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(row)
        return rows

    @staticmethod
    def _canonical_name(app_name: str) -> str:
        text = DesktopAppProfileRegistry._normalize_text(app_name)
        text = re.sub(r"\bversion\b.*$", "", text).strip()
        text = re.sub(r"\b\d+(?:\.\d+)+(?:\.\d+)?\b", "", text).strip()
        text = re.sub(r"\b(x64|x86|64-bit|32-bit|user)\b", "", text).strip()
        return re.sub(r"\s+", " ", text).strip() or DesktopAppProfileRegistry._normalize_text(app_name)

    @staticmethod
    def _keyword_aliases(canonical_name: str, app_name: str) -> List[str]:
        aliases = [canonical_name]
        normalized_name = DesktopAppProfileRegistry._normalize_text(app_name)
        if "visual studio code" in normalized_name:
            aliases.extend(["vscode", "code"])
        if "visual studio community" in normalized_name:
            aliases.extend(["visual studio", "vs"])
        if "google chrome" in normalized_name:
            aliases.append("chrome")
        if "microsoft edge" in normalized_name:
            aliases.append("edge")
        if "windows terminal" in normalized_name:
            aliases.append("terminal")
        if "powershell" in normalized_name:
            aliases.append("pwsh")
        return aliases

    def _package_id_aliases(self, package_ids: List[str]) -> List[str]:
        aliases: List[str] = []
        for package_id in package_ids:
            normalized_id = self._normalize_text(package_id)
            if not normalized_id:
                continue
            aliases.append(normalized_id.replace(".", " "))
            parts = [part for part in re.split(r"[./_\\-]+", str(package_id or "").strip()) if part.strip()]
            cleaned = [self._humanize_identifier(part) for part in parts if self._normalize_text(part) not in self.PACKAGE_ID_STOPWORDS]
            if cleaned and (len(cleaned) == 1 or cleaned[-1] == cleaned[-2]):
                aliases.append(cleaned[-1])
            if len(cleaned) >= 2:
                aliases.append(" ".join(cleaned[-2:]))
        return _dedupe_strings(aliases)

    def _package_id_exe_hints(self, package_ids: List[str]) -> List[str]:
        rows: List[str] = []
        for package_id in package_ids:
            parts = [part for part in re.split(r"[./_\\-]+", str(package_id or "").strip()) if part.strip()]
            cleaned = [part for part in parts if self._normalize_text(part) not in self.PACKAGE_ID_STOPWORDS]
            if not cleaned:
                continue
            candidate = cleaned[-2] if cleaned[-1].lower() == "exe" and len(cleaned) >= 2 else cleaned[-1]
            candidate_slug = self._slug(self._humanize_identifier(candidate))
            if candidate_slug:
                rows.append(f"{candidate_slug}.exe")
        return _dedupe_strings(rows)

    @staticmethod
    def _exe_hints(canonical_name: str, aliases: List[str]) -> List[str]:
        rows: List[str] = []
        slug = DesktopAppProfileRegistry._slug(canonical_name)
        if slug:
            rows.append(f"{slug}.exe")
        for alias in aliases:
            alias_slug = DesktopAppProfileRegistry._slug(alias)
            if alias_slug:
                rows.append(f"{alias_slug}.exe")
        return rows

    def _score_profile(self, profile: Dict[str, Any], *, app_name: str, window_title: str, exe_name: str) -> Tuple[float, List[str]]:
        score = 0.0
        reasons: List[str] = []
        aliases = [self._normalize_text(alias) for alias in profile.get("aliases", []) if str(alias).strip()]
        title_hints = [self._normalize_text(alias) for alias in profile.get("window_title_hints", []) if str(alias).strip()]
        exe_hints = [self._normalize_text(alias) for alias in profile.get("exe_hints", []) if str(alias).strip()]
        package_ids = [self._normalize_text(package_id) for package_id in profile.get("package_ids", []) if str(package_id).strip()]

        def promote(next_score: float, reason: str) -> None:
            nonlocal score
            if next_score <= score:
                return
            score = next_score
            if reason not in reasons:
                reasons.append(reason)

        for alias in aliases:
            if app_name and app_name == alias:
                promote(1.0, "alias_exact")
            elif app_name and alias and (alias in app_name or app_name in alias):
                promote(0.9, "alias_partial")
            if window_title and alias and alias in window_title:
                promote(0.82, "window_alias")
        for hint in title_hints:
            if window_title and hint and hint in window_title:
                promote(0.8, "window_hint")
        for hint in exe_hints:
            if exe_name and hint == exe_name:
                promote(0.98, "exe_exact")
            elif exe_name and hint and hint in exe_name:
                promote(0.88, "exe_partial")
        for package_id in package_ids:
            if app_name and package_id and package_id in app_name:
                promote(0.84, "package_id")
            if window_title and package_id and package_id in window_title:
                promote(0.74, "package_window")
        return round(score, 6), reasons

    @staticmethod
    def _humanize_identifier(value: str) -> str:
        text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or "").strip())
        text = re.sub(r"[^A-Za-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip().lower()

    @staticmethod
    def _normalize_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[^a-z0-9.+#]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _slug(value: str) -> str:
        text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
        return text.strip("_")
