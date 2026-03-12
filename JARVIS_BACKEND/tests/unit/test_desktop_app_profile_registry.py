from __future__ import annotations

from pathlib import Path

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry


def test_desktop_app_profile_registry_parses_catalog_and_categories(tmp_path: Path) -> None:
    apps_a = tmp_path / "apps-a.txt"
    apps_b = tmp_path / "apps-b.txt"
    apps_a.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Google Chrome                             Google.Chrome.EXE            145.0                winget",
                "Cloudflare WARP                           Cloudflare.Warp              25.10.186             winget",
                "Warp                                      Warp.Warp                    v0.2026              winget",
            ]
        ),
        encoding="utf-8",
    )
    apps_b.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Discord                                   ARP\\User\\X64\\Discord       1.0.9225",
                "Google Chrome                             Google.Chrome.EXE            145.0                winget",
            ]
        ),
        encoding="utf-8",
    )

    registry = DesktopAppProfileRegistry(source_paths=[str(apps_a), str(apps_b)])
    catalog = registry.catalog(limit=10)

    assert catalog["status"] == "success"
    assert catalog["total"] == 4
    assert catalog["category_counts"]["browser"] == 1
    assert registry.match(app_name="Google Chrome")["category"] == "browser"
    assert registry.match(app_name="Cloudflare WARP")["category"] == "security"
    assert registry.match(app_name="Warp")["category"] == "terminal"
    discord = registry.match(exe_name="discord.exe")
    assert discord["status"] == "success"
    assert discord["category"] == "chat"
