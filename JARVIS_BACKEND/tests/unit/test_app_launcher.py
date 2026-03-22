from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict

from backend.python.database.local_store import LocalStore
from backend.python.pc_control.app_launcher import AppLauncher


class _StubRegistry:
    def __init__(self, profile: Dict[str, Any]) -> None:
        self._profile = dict(profile)

    def match(self, *, app_name: str = "", window_title: str = "", exe_name: str = "") -> Dict[str, Any]:
        return dict(self._profile)


def test_app_launcher_resolves_start_apps_entry_for_msix_alias(monkeypatch, tmp_path: Path) -> None:
    launcher = AppLauncher(
        profile_registry=_StubRegistry(
            {
                "status": "success",
                "name": "Windows Calculator",
                "canonical_name": "windows calculator",
                "category": "utility",
                "aliases": ["windows calculator", "calculator"],
                "exe_hints": ["calculator.exe"],
                "package_ids": ["MSIX\\Microsoft.WindowsCalculator_8wekyb3d8bbwe"],
                "window_title_hints": ["windows calculator", "calculator"],
            }
        )
    )
    launcher._memory_store = LocalStore(str(tmp_path / "app_launcher_memory.json"))

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: None)
    monkeypatch.setattr(shutil, "which", lambda value: None)
    monkeypatch.setattr(
        launcher,
        "_load_start_apps",
        lambda: [{"name": "Windows Calculator", "app_id": "Microsoft.WindowsCalculator_8wekyb3d8bbwe!App"}],
    )
    monkeypatch.setattr(launcher, "_load_start_menu_shortcuts", lambda: [])
    monkeypatch.setattr(launcher, "_load_uninstall_entries", lambda: [])
    monkeypatch.setattr(launcher, "_search_common_paths", lambda exe_name: None)

    target = launcher.resolve_launch_target("calculator")

    assert target["status"] == "success"
    assert target["kind"] == "apps_folder"
    assert target["app_id"] == "Microsoft.WindowsCalculator_8wekyb3d8bbwe!App"
    assert target["resolution"] == "start_apps"

    launched: list[Any] = []

    class _DummyProcess:
        pass

    monkeypatch.setattr(subprocess, "Popen", lambda args, **kwargs: launched.append(args) or _DummyProcess())

    result = launcher.launch("calculator")

    assert result["status"] == "success"
    assert result["launch_method"] == "apps_folder"
    assert launched == [["explorer.exe", r"shell:AppsFolder\Microsoft.WindowsCalculator_8wekyb3d8bbwe!App"]]


def test_app_launcher_resolves_app_paths_registry_before_other_fallbacks(monkeypatch, tmp_path: Path) -> None:
    launcher = AppLauncher(
        profile_registry=_StubRegistry(
            {
                "status": "success",
                "name": "Google Chrome",
                "canonical_name": "google chrome",
                "category": "browser",
                "aliases": ["google chrome", "chrome"],
                "exe_hints": ["chrome.exe"],
                "package_ids": ["Google.Chrome.EXE"],
                "window_title_hints": ["google chrome", "chrome"],
            }
        )
    )
    launcher._memory_store = LocalStore(str(tmp_path / "app_launcher_memory.json"))

    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: chrome_path if exe_name == "chrome.exe" else None)
    monkeypatch.setattr(shutil, "which", lambda value: None)
    monkeypatch.setattr(launcher, "_load_start_apps", lambda: [])
    monkeypatch.setattr(launcher, "_load_start_menu_shortcuts", lambda: [])
    monkeypatch.setattr(launcher, "_load_uninstall_entries", lambda: [])
    monkeypatch.setattr(launcher, "_search_common_paths", lambda exe_name: None)

    target = launcher.resolve_launch_target("chrome")

    assert target["status"] == "success"
    assert target["kind"] == "path"
    assert target["path"] == chrome_path
    assert target["resolution"] == "app_paths_registry"

    started: list[str] = []
    monkeypatch.setattr(os, "startfile", lambda path: started.append(path), raising=False)

    result = launcher.launch("chrome")

    assert result["status"] == "success"
    assert result["launch_method"] == "launch_memory"
    assert result["memory_hit"] is True
    assert started == [chrome_path]


def test_app_launcher_resolves_start_menu_shortcut_for_user_installed_app(monkeypatch, tmp_path: Path) -> None:
    launcher = AppLauncher(
        profile_registry=_StubRegistry(
            {
                "status": "success",
                "name": "Claude",
                "canonical_name": "claude",
                "category": "ai_companion",
                "aliases": ["claude"],
                "exe_hints": ["claude.exe"],
                "package_ids": ["Claude"],
                "window_title_hints": ["claude"],
            }
        )
    )
    launcher._memory_store = LocalStore(str(tmp_path / "app_launcher_memory.json"))

    shortcut = tmp_path / "Claude.lnk"
    shortcut.write_text("shortcut", encoding="utf-8")

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: None)
    monkeypatch.setattr(shutil, "which", lambda value: None)
    monkeypatch.setattr(launcher, "_load_start_apps", lambda: [])
    monkeypatch.setattr(launcher, "_load_start_menu_shortcuts", lambda: [{"name": "Claude", "path": str(shortcut)}])
    monkeypatch.setattr(launcher, "_load_uninstall_entries", lambda: [])
    monkeypatch.setattr(launcher, "_search_common_paths", lambda exe_name: None)

    target = launcher.resolve_launch_target("claude")

    assert target["status"] == "success"
    assert target["kind"] == "shortcut"
    assert target["path"] == str(shortcut)
    assert target["resolution"] == "start_menu_shortcut"


def test_app_launcher_resolves_uninstall_install_location_when_direct_path_is_missing(monkeypatch, tmp_path: Path) -> None:
    launcher = AppLauncher(
        profile_registry=_StubRegistry(
            {
                "status": "success",
                "name": "Docker Desktop",
                "canonical_name": "docker desktop",
                "category": "ops_console",
                "aliases": ["docker", "docker desktop"],
                "exe_hints": ["docker.exe"],
                "package_ids": ["Docker.DockerDesktop"],
                "window_title_hints": ["docker", "docker desktop"],
            }
        )
    )
    launcher._memory_store = LocalStore(str(tmp_path / "app_launcher_memory.json"))

    install_dir = tmp_path / "Docker"
    install_dir.mkdir()
    docker_exe = install_dir / "docker.exe"
    docker_exe.write_text("binary", encoding="utf-8")

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: None)
    monkeypatch.setattr(shutil, "which", lambda value: None)
    monkeypatch.setattr(launcher, "_load_start_apps", lambda: [])
    monkeypatch.setattr(launcher, "_load_start_menu_shortcuts", lambda: [])
    monkeypatch.setattr(
        launcher,
        "_load_uninstall_entries",
        lambda: [{"name": "Docker Desktop", "path": "", "install_location": str(install_dir)}],
    )
    monkeypatch.setattr(launcher, "_search_common_paths", lambda exe_name: None)

    target = launcher.resolve_launch_target("docker")

    assert target["status"] == "success"
    assert target["kind"] == "path"
    assert target["path"] == str(docker_exe)
    assert target["resolution"] == "uninstall_registry"


def test_app_launcher_reuses_launch_memory_and_exposes_it_in_inventory(monkeypatch, tmp_path: Path) -> None:
    launcher = AppLauncher(
        profile_registry=_StubRegistry(
            {
                "status": "success",
                "name": "Google Chrome",
                "canonical_name": "google chrome",
                "category": "browser",
                "aliases": ["google chrome", "chrome"],
                "exe_hints": ["chrome.exe"],
                "package_ids": ["Google.Chrome.EXE"],
                "window_title_hints": ["google chrome", "chrome"],
            }
        )
    )
    launcher._memory_store = LocalStore(str(tmp_path / "app_launcher_memory.json"))

    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: chrome_path if exe_name == "chrome.exe" else None)
    monkeypatch.setattr(shutil, "which", lambda value: None)
    monkeypatch.setattr(launcher, "_load_start_apps", lambda: [])
    monkeypatch.setattr(launcher, "_load_start_menu_shortcuts", lambda: [])
    monkeypatch.setattr(launcher, "_load_uninstall_entries", lambda: [])
    monkeypatch.setattr(launcher, "_load_app_paths_entries", lambda: [])
    monkeypatch.setattr(launcher, "_search_common_paths", lambda exe_name: None)
    monkeypatch.setattr(launcher, "_usage_summary", lambda: [])

    first = launcher.resolve_launch_target("chrome")
    assert first["status"] == "success"
    assert first["resolution"] == "app_paths_registry"

    monkeypatch.setattr(launcher, "_query_app_path", lambda exe_name: None)

    second = launcher.resolve_launch_target("chrome")
    assert second["status"] == "success"
    assert second["resolution"] == "launch_memory"
    assert second["memory_hit"] is True
    assert second["path"] == chrome_path

    memory = launcher.launch_memory_snapshot(query="chrome")
    assert memory["status"] == "success"
    assert memory["total"] >= 1
    assert memory["items"][0]["path"] == chrome_path

    inventory = launcher.inventory_snapshot(query="chrome")
    assert inventory["status"] == "success"
    assert inventory["total"] >= 1
    assert "launch_memory" in inventory["items"][0]["sources"]
    assert inventory["items"][0]["path"] == chrome_path
