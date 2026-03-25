from __future__ import annotations

import hashlib
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_text(value: Any) -> str:
    return _clean_text(value).lower()


def _dedupe_strings(values: Iterable[Any], *, limit: int = 12) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _clean_text(value)
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
        if len(rows) >= max(1, int(limit or 1)):
            break
    return rows


class DesktopVMManager:
    _PROVIDERS: Dict[str, Dict[str, Any]] = {
        "hyper_v": {
            "label": "Hyper-V",
            "aliases": ["hyper-v", "hyper v", "hyperv", "vmconnect"],
            "app_names": ["Hyper-V Manager", "Virtual Machine Connection"],
            "cli_candidates": ["vmconnect.exe", "powershell.exe"],
            "default_control_mode": "provider_console",
        },
        "virtualbox": {
            "label": "VirtualBox",
            "aliases": ["virtualbox", "oracle vm virtualbox", "vboxmanage"],
            "app_names": ["VirtualBox", "Oracle VM VirtualBox", "VirtualBox Manager"],
            "cli_candidates": ["VBoxManage.exe", "VirtualBoxVM.exe", "virtualbox.exe"],
            "default_control_mode": "provider_console",
        },
        "vmware_workstation": {
            "label": "VMware Workstation",
            "aliases": ["vmware", "vmware workstation", "vmware workstation pro", "vmrun"],
            "app_names": ["VMware Workstation", "VMware Workstation Pro", "VMware Player"],
            "cli_candidates": ["vmrun.exe", "vmware.exe"],
            "default_control_mode": "provider_console",
        },
        "qemu": {
            "label": "QEMU",
            "aliases": ["qemu", "virt-manager", "virsh"],
            "app_names": ["QEMU", "virt-manager", "Virt Manager", "Virsh"],
            "cli_candidates": ["qemu-system-x86_64.exe", "virsh.exe", "virt-manager.exe"],
            "default_control_mode": "provider_console",
        },
        "remote_desktop": {
            "label": "Remote Desktop",
            "aliases": ["remote desktop", "rdp", "mstsc"],
            "app_names": ["Remote Desktop Connection", "Microsoft Remote Desktop"],
            "cli_candidates": ["mstsc.exe"],
            "default_control_mode": "rdp",
        },
        "vnc": {
            "label": "VNC",
            "aliases": ["vnc", "tightvnc", "realvnc", "ultravnc"],
            "app_names": ["VNC Viewer", "RealVNC Viewer", "TightVNC Viewer", "UltraVNC Viewer"],
            "cli_candidates": ["vncviewer.exe", "tvnviewer.exe", "vncviewer64.exe"],
            "default_control_mode": "vnc",
        },
        "ssh_terminal": {
            "label": "SSH Terminal",
            "aliases": ["ssh", "openssh", "windows terminal", "terminal"],
            "app_names": ["Windows Terminal", "PowerShell", "Command Prompt"],
            "cli_candidates": ["ssh.exe", "wt.exe", "powershell.exe"],
            "default_control_mode": "ssh",
        },
    }

    def __init__(self, *, store_path: str = "data/desktop_vm_manager.json") -> None:
        self._store = LocalStore(store_path)

    def guest_profiles(self, *, limit: int = 200) -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 200), 2000))
        rows = self._store.get("guest_profiles", {})
        by_id = dict(rows) if isinstance(rows, dict) else {}
        items = [
            dict(item)
            for _, item in sorted(by_id.items(), key=lambda entry: str(entry[0]).lower())
            if isinstance(item, dict)
        ][:bounded]
        return {"status": "success", "count": len(items), "items": items, "by_id": by_id}

    def update_guest_profile(
        self,
        *,
        guest_name: str = "",
        provider: str = "",
        guest_os: str = "",
        control_mode: str = "",
        provider_app_name: str = "",
        provider_launch_target: str = "",
        remote_endpoint: str = "",
        enable_learning: Optional[bool] = None,
        notes: str = "",
        tags: Optional[Iterable[Any]] = None,
        credentials_ref: str = "",
        source: str = "manual",
    ) -> Dict[str, Any]:
        clean_guest_name = _clean_text(guest_name)
        clean_provider = self._normalize_provider(provider)
        if not clean_guest_name:
            return {"status": "error", "message": "guest_name is required"}
        if not clean_provider:
            return {"status": "error", "message": "provider is required"}
        guest_id = self._guest_id(clean_provider, clean_guest_name, remote_endpoint)
        profiles = self._store.get("guest_profiles", {})
        rows = dict(profiles) if isinstance(profiles, dict) else {}
        current = dict(rows.get(guest_id, {})) if isinstance(rows.get(guest_id, {}), dict) else {}
        catalog = self._PROVIDERS.get(clean_provider, {})
        updated = dict(current)
        updated.update(
            {
                "guest_id": guest_id,
                "guest_name": clean_guest_name,
                "provider": clean_provider,
                "provider_label": str(catalog.get("label", clean_provider.replace("_", " ").title())),
                "guest_os": _norm_text(guest_os),
                "control_mode": _norm_text(control_mode) or str(catalog.get("default_control_mode", "provider_console")),
                "provider_app_name": _clean_text(provider_app_name),
                "provider_launch_target": _clean_text(provider_launch_target),
                "remote_endpoint": _clean_text(remote_endpoint),
                "enable_learning": bool(current.get("enable_learning", True) if enable_learning is None else enable_learning),
                "notes": _clean_text(notes),
                "credentials_ref": _clean_text(credentials_ref),
                "tags": _dedupe_strings(tags or []),
                "source": _norm_text(source) or "manual",
                "updated_at": _utc_now_iso(),
            }
        )
        updated["created_at"] = str(current.get("created_at", "") or updated["updated_at"])
        rows[guest_id] = updated
        self._store.set("guest_profiles", rows)
        return {"status": "success", "guest": updated, "count": len(rows)}

    def inventory_snapshot(
        self,
        *,
        system_profile: Optional[Dict[str, Any]] = None,
        app_inventory: Optional[Dict[str, Any]] = None,
        launch_memory: Optional[Dict[str, Any]] = None,
        query: str = "",
        provider: str = "",
        guest_os: str = "",
        limit: int = 64,
        task: str = "",
        source: str = "api",
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 64), 512))
        clean_query = _norm_text(query)
        clean_provider = self._normalize_provider(provider)
        clean_guest_os = _norm_text(guest_os)
        providers = self._detect_providers(
            system_profile=dict(system_profile or {}) if isinstance(system_profile, dict) else {},
            app_inventory=dict(app_inventory or {}) if isinstance(app_inventory, dict) else {},
            launch_memory=dict(launch_memory or {}) if isinstance(launch_memory, dict) else {},
        )
        guests = self._annotated_guests(
            providers=providers,
            system_profile=dict(system_profile or {}) if isinstance(system_profile, dict) else {},
            task=_norm_text(task),
        )
        if clean_provider:
            guests = [row for row in guests if _norm_text(row.get("provider", "")) == clean_provider]
        if clean_guest_os:
            guests = [row for row in guests if clean_guest_os in _norm_text(row.get("guest_os", ""))]
        if clean_query:
            guests = [
                row
                for row in guests
                if clean_query in _norm_text(row.get("guest_name", ""))
                or clean_query in _norm_text(row.get("provider_label", ""))
                or clean_query in _norm_text(row.get("guest_os", ""))
                or clean_query in _norm_text(row.get("remote_endpoint", ""))
            ]
        guests.sort(key=self._guest_sort_key)
        selected = guests[:bounded]
        summary = self._inventory_summary(
            providers=providers,
            guests=selected,
            all_guests=guests,
            system_profile=dict(system_profile or {}) if isinstance(system_profile, dict) else {},
        )
        return {
            "status": "success",
            "captured_at": _utc_now_iso(),
            "source": _norm_text(source) or "api",
            "task": _norm_text(task),
            "query": _clean_text(query),
            "count": len(selected),
            "total": len(guests),
            "limit": bounded,
            "providers": providers,
            "items": selected,
            "summary": summary,
            "next_actions": self._inventory_next_actions(summary=summary, guests=selected),
            "recommendations": self._inventory_recommendations(summary=summary),
        }

    def build_vm_control_plan(
        self,
        *,
        inventory: Optional[Dict[str, Any]] = None,
        task: str = "",
        query: str = "",
        max_targets: int = 4,
        machine_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        guests = [
            dict(item)
            for item in (inventory or {}).get("items", [])
            if isinstance((inventory or {}).get("items", []), list) and isinstance(item, dict)
        ]
        clean_query = _norm_text(query)
        clean_task = _norm_text(task)
        targets: List[Dict[str, Any]] = []
        for row in guests:
            targets.append(
                self._plan_guest_target(
                    row,
                    task=clean_task,
                    query=clean_query,
                    machine_profile=dict(machine_profile or {}) if isinstance(machine_profile, dict) else {},
                )
            )
        targets.sort(key=lambda item: (-int(item.get("priority_score", 0) or 0), _norm_text(item.get("guest_name", ""))))
        selected = targets[: max(1, min(int(max_targets or 4), 16))]
        memory_underused_guest_count = len(
            [row for row in selected if _norm_text(row.get("memory_route_alignment_status", "")) == "underused"]
        )
        memory_followthrough_guest_count = len(
            [row for row in selected if bool(row.get("memory_followthrough_recommended", False))]
        )
        setup_guided_guest_count = len(
            [
                row
                for row in selected
                if bool(row.get("recent_setup_followthrough_recommended", False))
                or bool(row.get("recent_setup_followthrough_required", False))
                or (
                    isinstance(row.get("provider_model_readiness", {}), dict)
                    and (
                        bool(row.get("provider_model_readiness", {}).get("recent_setup_followthrough_recommended", False))
                        or bool(row.get("provider_model_readiness", {}).get("recent_setup_followthrough_required", False))
                    )
                )
            ]
        )
        continuation_guided_guest_count = len(
            [
                row
                for row in selected
                if bool(row.get("recent_continuation_recommended", False))
                or bool(row.get("recent_continuation_required", False))
                or int(row.get("recent_continuation_memory_followthrough_count", 0) or 0) > 0
                or (
                    isinstance(row.get("provider_model_readiness", {}), dict)
                    and (
                        bool(row.get("provider_model_readiness", {}).get("recent_continuation_recommended", False))
                        or bool(row.get("provider_model_readiness", {}).get("recent_continuation_required", False))
                        or int(
                            row.get("provider_model_readiness", {}).get(
                                "recent_continuation_memory_followthrough_count",
                                0,
                            )
                            or 0
                        )
                        > 0
                    )
                )
            ]
        )
        default_max_surface_waves = (
            6 if memory_underused_guest_count > 0 else 5 if memory_followthrough_guest_count > 0 else 4
        )
        if setup_guided_guest_count > 0:
            default_max_surface_waves = max(default_max_surface_waves, 5)
        if continuation_guided_guest_count > 0:
            default_max_surface_waves = max(default_max_surface_waves, 6)
        default_max_probe_controls = (
            5 if memory_underused_guest_count > 0 else 4 if memory_followthrough_guest_count > 0 else 3
        )
        if setup_guided_guest_count > 0:
            default_max_probe_controls = max(default_max_probe_controls, 4)
        if continuation_guided_guest_count > 0:
            default_max_probe_controls = max(default_max_probe_controls, 5)
        preferred_wave_actions = _dedupe_strings(
            [
                str(action_name).strip().lower()
                for row in selected
                if bool(row.get("memory_followthrough_recommended", False))
                for action_name in row.get("preferred_wave_actions", [])
                if isinstance(row.get("preferred_wave_actions", []), list) and str(action_name).strip()
            ],
            limit=8,
        )
        if not preferred_wave_actions:
            preferred_wave_actions = _dedupe_strings(
                [
                    str(action_name).strip().lower()
                    for row in selected
                    for action_name in row.get("preferred_wave_actions", [])
                    if isinstance(row.get("preferred_wave_actions", []), list) and str(action_name).strip()
                ],
                limit=8,
            )
        if setup_guided_guest_count > 0:
            preferred_wave_actions = _dedupe_strings(
                [*preferred_wave_actions, "focus_toolbar", "focus_search_box", "command"],
                limit=8,
            )
        if continuation_guided_guest_count > 0:
            preferred_wave_actions = _dedupe_strings(
                [*preferred_wave_actions, "focus_navigation_tree", "focus_list_surface", "focus_sidebar"],
                limit=8,
            )
        memory_mission_status_counts: Dict[str, int] = {}
        top_memory_mission_queries: Dict[str, int] = {}
        top_memory_mission_hotkeys: Dict[str, int] = {}
        for row in selected:
            memory_mission = dict(row.get("memory_mission", {})) if isinstance(row.get("memory_mission", {}), dict) else {}
            memory_mission_status = _norm_text(memory_mission.get("status", "")) or "cold"
            memory_mission_status_counts[memory_mission_status] = int(
                memory_mission_status_counts.get(memory_mission_status, 0) or 0
            ) + 1
            for query_hint in memory_mission.get("query_hints", []):
                clean_query_hint = _clean_text(query_hint)
                if clean_query_hint:
                    top_memory_mission_queries[clean_query_hint] = int(
                        top_memory_mission_queries.get(clean_query_hint, 0) or 0
                    ) + 1
            for hotkey_hint in memory_mission.get("hotkey_hints", []):
                clean_hotkey_hint = _clean_text(hotkey_hint)
                if clean_hotkey_hint:
                    top_memory_mission_hotkeys[clean_hotkey_hint] = int(
                        top_memory_mission_hotkeys.get(clean_hotkey_hint, 0) or 0
                    ) + 1
        return {
            "status": "success",
            "count": len(selected),
            "items": selected,
            "summary": {
                "ready_count": len([row for row in selected if _norm_text(row.get("readiness_status", "")) == "ready"]),
                "attention_count": len([row for row in selected if _norm_text(row.get("readiness_status", "")) == "attention"]),
                "blocked_count": len([row for row in selected if _norm_text(row.get("readiness_status", "")) == "blocked"]),
                "provider_counts": self._count_values(selected, "provider"),
                "control_mode_counts": self._count_values(selected, "control_mode"),
                "guest_os_counts": self._count_values(selected, "guest_os"),
                "guest_family_counts": self._count_values(selected, "guest_family"),
                "learning_profile_counts": self._count_values(selected, "guest_learning_profile"),
                "execution_mode_counts": self._count_values(selected, "execution_mode"),
                "runtime_band_counts": self._count_values(selected, "runtime_band_preference"),
                "expected_route_profile_counts": self._count_values(selected, "expected_route_profile"),
                "expected_model_preference_counts": self._count_values(selected, "expected_model_preference"),
                "ai_route_status_counts": self._count_values(selected, "ai_route_status"),
                "ai_route_runtime_band_counts": self._count_values(selected, "selected_ai_runtime_band"),
                "ai_route_profile_counts": self._count_values(selected, "selected_ai_route_profile"),
                "ai_route_provider_source_counts": self._count_values(selected, "selected_ai_provider_source"),
                "ai_route_confident_count": len(
                    [row for row in selected if float(row.get("ai_route_confidence", 0.0) or 0.0) >= 0.66]
                ),
                "ai_route_fallback_count": len(
                    [row for row in selected if bool(row.get("ai_route_fallback_applied", False))]
                ),
                "route_resolution_status_counts": self._count_values(selected, "route_resolution_status"),
                "remediation_kind_counts": self._count_values(selected, "remediation_kind"),
                "structured_memory_low_coverage_guest_count": len(
                    [
                        row
                        for row in selected
                        if isinstance(row.get("provider_model_readiness", {}), dict)
                        and int(row.get("provider_model_readiness", {}).get("structured_memory_low_coverage_count", 0) or 0) > 0
                    ]
                ),
                "structured_memory_semantic_ready_guest_count": len(
                    [
                        row
                        for row in selected
                        if isinstance(row.get("provider_model_readiness", {}), dict)
                        and int(row.get("provider_model_readiness", {}).get("structured_memory_semantic_ready_count", 0) or 0) > 0
                    ]
                ),
                "memory_guidance_status_counts": self._count_values(selected, "memory_guidance_status"),
                "memory_route_alignment_counts": self._count_values(selected, "memory_route_alignment_status"),
                "memory_guided_route_count": len([row for row in selected if bool(row.get("memory_guided_route", False))]),
                "memory_assisted_route_count": len(
                    [row for row in selected if bool(row.get("memory_assisted_route", False))]
                ),
                "memory_mission_status_counts": {
                    str(key): int(value)
                    for key, value in sorted(memory_mission_status_counts.items(), key=lambda entry: entry[0])
                },
                "memory_underused_guest_count": memory_underused_guest_count,
                "memory_followthrough_guest_count": memory_followthrough_guest_count,
                "setup_guided_guest_count": setup_guided_guest_count,
                "continuation_guided_guest_count": continuation_guided_guest_count,
                "top_memory_mission_queries": {
                    str(key): int(value)
                    for key, value in sorted(
                        top_memory_mission_queries.items(),
                        key=lambda entry: (-int(entry[1]), str(entry[0]).lower()),
                    )[:8]
                },
                "top_memory_mission_hotkeys": {
                    str(key): int(value)
                    for key, value in sorted(
                        top_memory_mission_hotkeys.items(),
                        key=lambda entry: (-int(entry[1]), str(entry[0]).lower()),
                    )[:8]
                },
                "setup_followup_guest_count": len(
                    [
                        row
                        for row in selected
                        if isinstance(row.get("provider_model_readiness", {}), dict)
                        and len(row.get("provider_model_readiness", {}).get("setup_followup_codes", [])) > 0
                    ]
                ),
                "focus_guest_names": [str(row.get("guest_name", "")).strip() for row in selected[:4] if str(row.get("guest_name", "")).strip()],
            },
            "defaults": {
                "auto_prepare_vm_controls": True,
                "vm_prepare_limit": max(1, min(len(selected) or 1, 4)),
                "enable_learning": True,
                "follow_surface_waves": True,
                "max_surface_waves": default_max_surface_waves,
                "probe_controls": True,
                "max_probe_controls": default_max_probe_controls,
                "memory_followthrough_enabled": bool(memory_followthrough_guest_count > 0),
                "setup_guided_guest_count": setup_guided_guest_count,
                "continuation_guided_guest_count": continuation_guided_guest_count,
                "memory_mission_status_counts": {
                    str(key): int(value)
                    for key, value in sorted(memory_mission_status_counts.items(), key=lambda entry: entry[0])
                },
                "preferred_wave_actions": preferred_wave_actions[:8],
            },
            "next_actions": [
                {
                    "id": f"vm_prepare:{row.get('guest_id', '')}",
                    "kind": "deepen_vm_control_learning" if bool(row.get("memory_followthrough_recommended", False)) else "prepare_vm_control",
                    "title": (
                        f"Deepen VM learning for {_clean_text(row.get('guest_name', 'guest'))}"
                        if bool(row.get("memory_followthrough_recommended", False))
                        else f"Prepare VM control for {_clean_text(row.get('guest_name', 'guest'))}"
                    ),
                    "target": _clean_text(row.get("guest_name", "guest")),
                    "status": "ready" if bool(row.get("auto_prepare_allowed", False)) else "attention",
                    "recommended_action_code": _clean_text(row.get("remediation_action_code", "")),
                    "memory_followthrough_recommended": bool(row.get("memory_followthrough_recommended", False)),
                    "query_hints": list(
                        dict(row.get("memory_mission", {})).get("query_hints", [])
                        if isinstance(row.get("memory_mission", {}), dict)
                        and isinstance(dict(row.get("memory_mission", {})).get("query_hints", []), list)
                        else []
                    )[:8],
                    "hotkey_hints": list(
                        dict(row.get("memory_mission", {})).get("hotkey_hints", [])
                        if isinstance(row.get("memory_mission", {}), dict)
                        and isinstance(dict(row.get("memory_mission", {})).get("hotkey_hints", []), list)
                        else []
                    )[:8],
                }
                for row in selected[:4]
            ],
        }

    def prepare_guest_control(
        self,
        *,
        inventory: Optional[Dict[str, Any]] = None,
        guest_name: str = "",
        guest_id: str = "",
        app_launcher: Any = None,
        ensure_provider_launch: bool = True,
        query: str = "",
        source: str = "api",
        task: str = "",
        machine_profile: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        guests = [
            dict(item)
            for item in (inventory or {}).get("items", [])
            if isinstance((inventory or {}).get("items", []), list) and isinstance(item, dict)
        ]
        target = self._select_guest(guests, guest_name=guest_name, guest_id=guest_id)
        if not target:
            return {"status": "error", "message": "guest_name or guest_id not found"}
        target = self._plan_guest_target(
            target,
            task=_norm_text(task),
            query=_norm_text(query),
            machine_profile=dict(machine_profile or {}) if isinstance(machine_profile, dict) else {},
        )
        provider_launch_target = (
            _clean_text(target.get("provider_launch_target", ""))
            or _clean_text(target.get("provider_app_name", ""))
            or _clean_text(target.get("provider_detected_app_name", ""))
            or self._default_launch_target(target)
        )
        resolved_target: Dict[str, Any] = {}
        launch_payload: Dict[str, Any] = {"status": "skipped"}
        if provider_launch_target and app_launcher is not None:
            try:
                resolved_target = app_launcher.resolve_launch_target(provider_launch_target)
            except Exception as exc:  # noqa: BLE001
                resolved_target = {"status": "error", "message": str(exc), "requested_app": provider_launch_target}
            if ensure_provider_launch and isinstance(resolved_target, dict) and resolved_target.get("status") == "success":
                try:
                    launch_payload = app_launcher.launch(provider_launch_target)
                except Exception as exc:  # noqa: BLE001
                    launch_payload = {"status": "error", "message": str(exc), "requested_app": provider_launch_target}
        readiness_status = _norm_text(target.get("readiness_status", "")) or "attention"
        if readiness_status == "blocked":
            status = "blocked"
        elif _norm_text(launch_payload.get("status", "")) in {"success", "partial"} or not ensure_provider_launch:
            status = "success"
        elif provider_launch_target and isinstance(resolved_target, dict) and resolved_target.get("status") == "success":
            status = "partial"
        else:
            status = "attention"
        self._record_prepare_status(target=target, status=status, source=source)
        return {
            "status": status,
            "guest": target,
            "resolved_target": resolved_target,
            "launch": launch_payload,
            "summary": {
                "guest_name": _clean_text(target.get("guest_name", "")),
                "provider": _norm_text(target.get("provider", "")),
                "provider_label": _clean_text(target.get("provider_label", "")),
                "guest_os": _norm_text(target.get("guest_os", "")),
                "control_mode": _norm_text(target.get("control_mode", "")),
                "readiness_status": readiness_status,
                "prepare_priority_band": _norm_text(target.get("prepare_priority_band", "")),
                "remote_endpoint": _clean_text(target.get("remote_endpoint", "")),
                "provider_launch_target": provider_launch_target,
                "provider_launch_ready": bool(isinstance(resolved_target, dict) and resolved_target.get("status") == "success"),
                "launch_status": _norm_text(launch_payload.get("status", "")) or "skipped",
                "attach_strategy": self._attach_strategy(target),
                "learning_query": _clean_text(query) or self._guest_learning_query(target),
                "guest_family": _norm_text(target.get("guest_family", "")),
                "guest_learning_profile": _norm_text(target.get("guest_learning_profile", "")),
                "execution_mode": _norm_text(target.get("execution_mode", "")),
                "runtime_band_preference": _norm_text(target.get("runtime_band_preference", "")),
                "expected_route_profile": _norm_text(target.get("expected_route_profile", "")),
                "expected_model_preference": _norm_text(target.get("expected_model_preference", "")),
                "ai_route_status": _norm_text(target.get("ai_route_status", "")),
                "ai_route_confidence": float(target.get("ai_route_confidence", 0.0) or 0.0),
                "selected_ai_runtime_band": _norm_text(target.get("selected_ai_runtime_band", "")),
                "selected_ai_route_profile": _norm_text(target.get("selected_ai_route_profile", "")),
                "selected_ai_provider_source": _norm_text(target.get("selected_ai_provider_source", "")),
                "memory_guidance_status": _norm_text(target.get("memory_guidance_status", "")),
                "memory_guidance_reason_codes": list(target.get("memory_guidance_reason_codes", []))
                if isinstance(target.get("memory_guidance_reason_codes", []), list)
                else [],
                "memory_guided_route": bool(target.get("memory_guided_route", False)),
                "memory_assisted_route": bool(target.get("memory_assisted_route", False)),
                "memory_mission": (
                    dict(target.get("memory_mission", {}))
                    if isinstance(target.get("memory_mission", {}), dict)
                    else {}
                ),
                "memory_route_alignment_status": _norm_text(target.get("memory_route_alignment_status", "")),
                "memory_route_reason_codes": list(target.get("memory_route_reason_codes", []))
                if isinstance(target.get("memory_route_reason_codes", []), list)
                else [],
                "route_resolution_status": _norm_text(target.get("route_resolution_status", "")),
                "remediation_kind": _norm_text(target.get("remediation_kind", "")),
                "remediation_action_code": _clean_text(target.get("remediation_action_code", "")),
                "recommended_traversal_roles": list(target.get("recommended_traversal_roles", []))
                if isinstance(target.get("recommended_traversal_roles", []), list)
                else [],
                "preferred_wave_actions": list(target.get("preferred_wave_actions", []))
                if isinstance(target.get("preferred_wave_actions", []), list)
                else [],
                "recommended_traversal_paths": list(target.get("recommended_traversal_paths", []))
                if isinstance(target.get("recommended_traversal_paths", []), list)
                else [],
                "recommended_max_surface_waves": int(target.get("recommended_max_surface_waves", 4) or 4),
                "recommended_max_probe_controls": int(target.get("recommended_max_probe_controls", 3) or 3),
                "memory_followthrough_recommended": bool(target.get("memory_followthrough_recommended", False)),
                "capability_tags": list(target.get("capability_tags", []))
                if isinstance(target.get("capability_tags", []), list)
                else [],
                "provider_model_readiness": dict(target.get("provider_model_readiness", {}))
                if isinstance(target.get("provider_model_readiness", {}), dict)
                else {},
                "reason_codes": list(target.get("reason_codes", [])) if isinstance(target.get("reason_codes", []), list) else [],
                "blocker_codes": list(target.get("blocker_codes", [])) if isinstance(target.get("blocker_codes", []), list) else [],
                "memory_guided_route_count": 1 if bool(target.get("memory_guided_route", False)) else 0,
                "memory_assisted_route_count": 1 if bool(target.get("memory_assisted_route", False)) else 0,
                "memory_route_alignment_counts": {
                    _norm_text(target.get("memory_route_alignment_status", "")) or "cold": 1
                },
            },
            "message": "Prepared a host-side virtual machine control route."
            if status in {"success", "partial"}
            else "Virtual machine control needs more setup before JARVIS can attach safely.",
        }

    @classmethod
    def _normalize_provider(cls, value: Any) -> str:
        clean = _norm_text(value)
        if not clean:
            return ""
        for provider_id, row in cls._PROVIDERS.items():
            candidates = [provider_id, str(row.get("label", "")), *[str(item) for item in row.get("aliases", [])]]
            if any(clean == _norm_text(item) for item in candidates):
                return provider_id
        return clean.replace(" ", "_")

    @staticmethod
    def _guest_id(provider: str, guest_name: str, remote_endpoint: str) -> str:
        seed = "|".join([_norm_text(provider), _norm_text(guest_name), _norm_text(remote_endpoint)])
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    @classmethod
    def _detect_providers(
        cls,
        *,
        system_profile: Dict[str, Any],
        app_inventory: Dict[str, Any],
        launch_memory: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        virtualization = dict(system_profile.get("virtualization", {})) if isinstance(system_profile.get("virtualization", {}), dict) else {}
        virtualization_enabled = bool(virtualization.get("virtualization_firmware_enabled", False))
        inventory_items = app_inventory.get("items", []) if isinstance(app_inventory.get("items", []), list) else []
        launch_items = launch_memory.get("items", []) if isinstance(launch_memory.get("items", []), list) else []
        rows: List[Dict[str, Any]] = []
        for provider_id, row in cls._PROVIDERS.items():
            aliases = [_norm_text(row.get("label", "")), *[_norm_text(item) for item in row.get("aliases", [])], *[_norm_text(item) for item in row.get("app_names", [])]]
            matched_app_names: List[str] = []
            for item in inventory_items:
                if not isinstance(item, dict):
                    continue
                haystacks = [_norm_text(item.get("display_name", "")), _norm_text(item.get("canonical_name", "")), _norm_text(item.get("path", ""))]
                if any(alias and any(alias in haystack for haystack in haystacks if haystack) for alias in aliases):
                    matched_app_names.append(_clean_text(item.get("display_name", "")) or _clean_text(item.get("canonical_name", "")))
            memory_hits: List[str] = []
            for item in launch_items:
                if not isinstance(item, dict):
                    continue
                haystacks = [_norm_text(item.get("display_name", "")), _norm_text(item.get("requested_app", "")), _norm_text(item.get("path", ""))]
                if any(alias and any(alias in haystack for haystack in haystacks if haystack) for alias in aliases):
                    memory_hits.append(_clean_text(item.get("display_name", "")) or _clean_text(item.get("requested_app", "")))
            cli_hits = [str(shutil.which(candidate) or "").strip() for candidate in row.get("cli_candidates", []) if str(shutil.which(candidate) or "").strip()]
            detected = bool(matched_app_names or memory_hits or cli_hits)
            readiness_status = "ready" if detected else "attention"
            if provider_id in {"hyper_v", "virtualbox", "vmware_workstation", "qemu"} and not virtualization_enabled and not detected:
                readiness_status = "attention"
            rows.append(
                {
                    "provider": provider_id,
                    "provider_label": str(row.get("label", provider_id.replace("_", " ").title())),
                    "readiness_status": readiness_status,
                    "detected": detected,
                    "matched_app_names": _dedupe_strings(matched_app_names, limit=6),
                    "memory_hits": _dedupe_strings(memory_hits, limit=6),
                    "cli_hits": _dedupe_strings(cli_hits, limit=6),
                    "default_control_mode": str(row.get("default_control_mode", "provider_console")),
                    "reason_codes": ["provider_detected"] if detected else ["provider_not_detected"],
                }
            )
        return sorted(rows, key=lambda item: _norm_text(item.get("provider_label", "")))

    def _annotated_guests(self, *, providers: List[Dict[str, Any]], system_profile: Dict[str, Any], task: str) -> List[Dict[str, Any]]:
        provider_map = {_norm_text(item.get("provider", "")): dict(item) for item in providers if isinstance(item, dict)}
        virtualization = dict(system_profile.get("virtualization", {})) if isinstance(system_profile.get("virtualization", {}), dict) else {}
        virtualization_enabled = bool(virtualization.get("virtualization_firmware_enabled", False))
        rows: List[Dict[str, Any]] = []
        for guest in self.guest_profiles(limit=512).get("items", []):
            if not isinstance(guest, dict):
                continue
            row = dict(guest)
            provider = self._normalize_provider(row.get("provider", ""))
            provider_row = provider_map.get(provider, {})
            remote_endpoint = _clean_text(row.get("remote_endpoint", ""))
            provider_detected = bool(provider_row.get("detected", False))
            blocker_codes: List[str] = []
            if provider in {"hyper_v", "virtualbox", "vmware_workstation", "qemu"} and not virtualization_enabled and not provider_detected:
                blocker_codes.append("virtualization_disabled")
            if not provider_detected and not remote_endpoint and not _clean_text(row.get("provider_app_name", "")) and not _clean_text(row.get("provider_launch_target", "")):
                blocker_codes.append("provider_not_detected")
            readiness_status = "blocked" if blocker_codes else "ready" if (provider_detected or remote_endpoint) else "attention"
            reason_codes = [
                "learning_enabled" if bool(row.get("enable_learning", False)) else "",
                "provider_detected" if provider_detected else "",
                "remote_endpoint" if remote_endpoint else "",
                "task_match" if task and task in _norm_text(row.get("guest_os", "")) else "",
            ]
            rows.append(
                {
                    **row,
                    "provider": provider,
                    "provider_label": str(provider_row.get("provider_label", row.get("provider_label", provider.replace("_", " ").title()))),
                    "provider_detected": provider_detected,
                    "provider_detected_app_name": (
                        list(provider_row.get("matched_app_names", []))[0]
                        if isinstance(provider_row.get("matched_app_names", []), list) and provider_row.get("matched_app_names", [])
                        else ""
                    ),
                    "control_mode": _norm_text(row.get("control_mode", "")) or _norm_text(provider_row.get("default_control_mode", "")) or "provider_console",
                    "readiness_status": readiness_status,
                    "blocker_codes": _dedupe_strings(blocker_codes, limit=8),
                    "reason_codes": _dedupe_strings(reason_codes, limit=8),
                    "suggested_control_modes": self._suggested_control_modes(row, provider_row),
                    "guest_family": self._guest_family(row),
                    "learning_query": self._guest_learning_query(row),
                }
            )
        return rows

    @staticmethod
    def _guest_sort_key(row: Dict[str, Any]) -> tuple[int, int, str]:
        readiness_rank = {"ready": 0, "attention": 1, "blocked": 2}.get(_norm_text(row.get("readiness_status", "")), 3)
        learning_rank = 0 if bool(row.get("enable_learning", False)) else 1
        return (readiness_rank, learning_rank, _norm_text(row.get("provider_label", "")), _norm_text(row.get("guest_name", "")))

    @staticmethod
    def _inventory_summary(
        *,
        providers: List[Dict[str, Any]],
        guests: List[Dict[str, Any]],
        all_guests: List[Dict[str, Any]],
        system_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        virtualization = dict(system_profile.get("virtualization", {})) if isinstance(system_profile.get("virtualization", {}), dict) else {}
        return {
            "provider_count": len(providers),
            "detected_provider_count": len([row for row in providers if bool(row.get("detected", False))]),
            "ready_provider_count": len([row for row in providers if _norm_text(row.get("readiness_status", "")) == "ready"]),
            "guest_count": len(all_guests),
            "ready_guest_count": len([row for row in all_guests if _norm_text(row.get("readiness_status", "")) == "ready"]),
            "attention_guest_count": len([row for row in all_guests if _norm_text(row.get("readiness_status", "")) == "attention"]),
            "blocked_guest_count": len([row for row in all_guests if _norm_text(row.get("readiness_status", "")) == "blocked"]),
            "learning_enabled_guest_count": len([row for row in all_guests if bool(row.get("enable_learning", False))]),
            "provider_guest_counts": DesktopVMManager._count_values(all_guests, "provider"),
            "control_mode_counts": DesktopVMManager._count_values(all_guests, "control_mode"),
            "guest_os_counts": DesktopVMManager._count_values(all_guests, "guest_os"),
            "guest_family_counts": DesktopVMManager._count_values(all_guests, "guest_family"),
            "focus_guest_names": [str(row.get("guest_name", "")).strip() for row in guests[:4] if str(row.get("guest_name", "")).strip()],
            "virtualization_firmware_enabled": bool(virtualization.get("virtualization_firmware_enabled", False)),
            "wsl_available": bool(virtualization.get("wsl_available", False)),
        }

    @staticmethod
    def _inventory_next_actions(*, summary: Dict[str, Any], guests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        actions: List[Dict[str, Any]] = []
        if int(summary.get("guest_count", 0) or 0) <= 0:
            actions.append(
                {
                    "id": "vm:add_guest_profiles",
                    "kind": "add_vm_guest_profiles",
                    "title": "Add guest VM profiles",
                    "target": "guest_profiles",
                    "status": "manual_input_required",
                }
            )
        if int(summary.get("detected_provider_count", 0) or 0) <= 0:
            actions.append(
                {
                    "id": "vm:install_provider",
                    "kind": "install_or_register_vm_provider",
                    "title": "Install or register a VM provider",
                    "target": "vm_provider",
                    "status": "attention",
                }
            )
        for row in guests[:3]:
            if not isinstance(row, dict) or _norm_text(row.get("readiness_status", "")) != "blocked":
                continue
            actions.append(
                {
                    "id": f"vm:blocker:{row.get('guest_id', '')}",
                    "kind": "resolve_vm_control_blocker",
                    "title": f"Resolve VM blockers for {_clean_text(row.get('guest_name', 'guest'))}",
                    "target": _clean_text(row.get("guest_name", "guest")),
                    "status": "attention",
                }
            )
        return actions[:6]

    @staticmethod
    def _inventory_recommendations(*, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not bool(summary.get("virtualization_firmware_enabled", False)):
            rows.append(
                {
                    "code": "enable_virtual_machine_support",
                    "severity": "medium",
                    "title": "Enable virtualization support for local VM providers",
                    "message": "Hyper-V, VirtualBox, VMware, and QEMU guests will be limited until virtualization support is enabled.",
                }
            )
        if int(summary.get("detected_provider_count", 0) or 0) <= 0:
            rows.append(
                {
                    "code": "install_or_register_vm_provider",
                    "severity": "high",
                    "title": "Install or register a VM provider",
                    "message": "JARVIS needs a detected VM provider or remote-control tool before it can prepare guest control routes.",
                }
            )
        if int(summary.get("guest_count", 0) or 0) <= 0:
            rows.append(
                {
                    "code": "register_virtual_machine_profiles",
                    "severity": "medium",
                    "title": "Register guest virtual machines",
                    "message": "Add your guest OSes and control modes so JARVIS can prepare them for cross-OS control and learning.",
                }
            )
        return rows

    @staticmethod
    def _count_values(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            clean = _norm_text(row.get(key, ""))
            if not clean:
                continue
            counts[clean] = int(counts.get(clean, 0) or 0) + 1
        return {str(name): int(value) for name, value in sorted(counts.items(), key=lambda item: item[0])}

    @staticmethod
    def _memory_route_guidance_summary(
        *,
        route_profile: str = "",
        memory_guidance_status: str = "",
        reason_codes: Optional[Iterable[Any]] = None,
    ) -> Dict[str, Any]:
        clean_route_profile = _norm_text(route_profile)
        clean_guidance_status = _norm_text(memory_guidance_status)
        clean_reason_codes = _dedupe_strings(
            [str(item).strip().lower() for item in (reason_codes or []) if str(item).strip()],
            limit=12,
        )
        memory_guided_route = bool(
            clean_route_profile.startswith("memory_guided_") or clean_route_profile == "accessibility_memory_first"
        )
        memory_assisted_route = bool(
            not memory_guided_route
            and any(
                token in reason
                for reason in clean_reason_codes
                for token in (
                    "memory_assisted_vm_route",
                    "semantic_memory_ready",
                    "vector_memory_available",
                    "learning_semantic_guidance_available",
                )
            )
        )
        if clean_guidance_status not in {"strong", "partial", "cold"}:
            if memory_guided_route:
                clean_guidance_status = "strong"
            elif memory_assisted_route:
                clean_guidance_status = "partial"
            else:
                clean_guidance_status = "cold"
        alignment_status = "cold"
        if clean_guidance_status in {"strong", "partial"} and memory_guided_route:
            alignment_status = "aligned"
        elif clean_guidance_status in {"strong", "partial"} and memory_assisted_route:
            alignment_status = "assisted"
        elif clean_guidance_status in {"strong", "partial"}:
            alignment_status = "underused"
        elif memory_guided_route or memory_assisted_route:
            alignment_status = "speculative"
        return {
            "memory_guided_route": memory_guided_route,
            "memory_assisted_route": memory_assisted_route,
            "memory_route_alignment_status": alignment_status,
            "memory_route_reason_codes": _dedupe_strings(
                [
                    *clean_reason_codes,
                    *(["memory_guided_route"] if memory_guided_route else []),
                    *(["memory_assisted_route"] if memory_assisted_route else []),
                    *(["memory_guidance_" + clean_guidance_status] if clean_guidance_status else []),
                    *(["memory_alignment_" + alignment_status] if alignment_status else []),
                ],
                limit=12,
            ),
        }

    @staticmethod
    def _select_guest(guests: List[Dict[str, Any]], *, guest_name: str, guest_id: str) -> Dict[str, Any]:
        clean_guest_id = _clean_text(guest_id)
        clean_guest_name = _norm_text(guest_name)
        if clean_guest_id:
            for row in guests:
                if _clean_text(row.get("guest_id", "")) == clean_guest_id:
                    return dict(row)
        if clean_guest_name:
            for row in guests:
                if clean_guest_name == _norm_text(row.get("guest_name", "")):
                    return dict(row)
            for row in guests:
                if clean_guest_name in _norm_text(row.get("guest_name", "")):
                    return dict(row)
        return {}

    @staticmethod
    def _suggested_control_modes(row: Dict[str, Any], provider_row: Dict[str, Any]) -> List[str]:
        modes = [_clean_text(row.get("control_mode", "")), _clean_text(provider_row.get("default_control_mode", ""))]
        guest_os = _norm_text(row.get("guest_os", ""))
        if "windows" in guest_os:
            modes.extend(["rdp", "provider_console"])
        if any(token in guest_os for token in ("linux", "ubuntu", "debian", "fedora", "bsd")):
            modes.extend(["ssh", "provider_console", "vnc"])
        return _dedupe_strings([mode.lower() for mode in modes if mode], limit=4)

    @staticmethod
    def _default_launch_target(target: Dict[str, Any]) -> str:
        control_mode = _norm_text(target.get("control_mode", ""))
        if control_mode == "rdp":
            return "Remote Desktop Connection"
        if control_mode == "ssh":
            return "Windows Terminal"
        if control_mode == "vnc":
            return "VNC Viewer"
        return _clean_text(target.get("provider_label", ""))

    @staticmethod
    def _attach_strategy(target: Dict[str, Any]) -> str:
        control_mode = _norm_text(target.get("control_mode", ""))
        return control_mode if control_mode in {"rdp", "vnc", "ssh"} else "provider_console"

    @staticmethod
    def _guest_family(row: Dict[str, Any]) -> str:
        guest_os = _norm_text(row.get("guest_os", ""))
        control_mode = _norm_text(row.get("control_mode", ""))
        if control_mode == "ssh":
            return "terminal"
        if "windows" in guest_os:
            return "windows_desktop"
        if any(token in guest_os for token in ("linux", "ubuntu", "debian", "fedora", "arch", "mint")):
            return "linux_desktop"
        if any(token in guest_os for token in ("bsd",)):
            return "unix_desktop"
        if any(token in guest_os for token in ("mac", "darwin", "osx")):
            return "macos_desktop"
        if control_mode in {"rdp", "vnc"}:
            return "remote_desktop"
        return "generic_guest"

    @classmethod
    def _expected_route_profile(cls, row: Dict[str, Any], *, task: str = "") -> str:
        control_mode = _norm_text(row.get("control_mode", ""))
        family = cls._guest_family(row)
        clean_task = _norm_text(task)
        if control_mode == "ssh":
            return "terminal_first_guest_control"
        if control_mode == "rdp":
            return "rdp_guest_desktop_control"
        if control_mode == "vnc":
            return "vnc_guest_desktop_control"
        if family == "windows_desktop":
            return "windows_vm_desktop_control"
        if family in {"linux_desktop", "unix_desktop"} and "code" in clean_task:
            return "linux_vm_workspace_control"
        if family in {"linux_desktop", "unix_desktop"}:
            return "linux_vm_desktop_control"
        if family == "macos_desktop":
            return "macos_vm_desktop_control"
        return "generic_vm_control"

    @classmethod
    def _expected_model_preference(cls, row: Dict[str, Any], *, task: str = "") -> str:
        control_mode = _norm_text(row.get("control_mode", ""))
        family = cls._guest_family(row)
        clean_task = _norm_text(task)
        if control_mode == "ssh":
            return "terminal_reasoning"
        if family == "macos_desktop":
            return "api_vision_runtime"
        if "vision" in clean_task or control_mode in {"rdp", "vnc"}:
            return "hybrid_runtime"
        if family in {"windows_desktop", "linux_desktop", "unix_desktop"}:
            return "hybrid_runtime"
        return "balanced_runtime"

    @classmethod
    def _runtime_band_preference(cls, row: Dict[str, Any]) -> str:
        control_mode = _norm_text(row.get("control_mode", ""))
        family = cls._guest_family(row)
        if control_mode == "ssh":
            return "local"
        if control_mode in {"rdp", "provider_console"} and family in {"windows_desktop", "linux_desktop", "unix_desktop"}:
            return "hybrid"
        if control_mode == "vnc" or family == "macos_desktop":
            return "api"
        return "hybrid"

    @classmethod
    def _guest_learning_profile(cls, row: Dict[str, Any], *, task: str = "") -> str:
        family = cls._guest_family(row)
        control_mode = _norm_text(row.get("control_mode", ""))
        clean_task = _norm_text(task)
        if control_mode == "ssh":
            return "terminal_bootstrap" if "setup" in clean_task or "install" in clean_task else "terminal_revalidate"
        if family == "windows_desktop":
            return "windows_desktop_explore"
        if family in {"linux_desktop", "unix_desktop"}:
            return "linux_workspace_explore" if any(token in clean_task for token in ("code", "dev", "workspace")) else "linux_desktop_explore"
        if family == "macos_desktop":
            return "macos_desktop_explore"
        if control_mode in {"rdp", "vnc"}:
            return "remote_desktop_revalidate"
        return "generic_guest_revalidate"

    @classmethod
    def _recommended_traversal_roles(cls, row: Dict[str, Any]) -> List[str]:
        family = cls._guest_family(row)
        control_mode = _norm_text(row.get("control_mode", ""))
        roles: List[str] = []
        if family == "terminal":
            roles.extend(["terminal", "dialog", "list"])
        elif family == "windows_desktop":
            roles.extend(["menu", "toolbar", "ribbon", "sidebar", "tree", "list", "dialog"])
        elif family in {"linux_desktop", "unix_desktop"}:
            roles.extend(["menu", "toolbar", "sidebar", "tree", "list", "dialog", "terminal"])
        elif family == "macos_desktop":
            roles.extend(["menu", "toolbar", "sidebar", "list", "dialog"])
        else:
            roles.extend(["menu", "toolbar", "list", "dialog"])
        if control_mode in {"rdp", "vnc"}:
            roles.append("window_switcher")
        return _dedupe_strings([role.lower() for role in roles], limit=8)

    @classmethod
    def _preferred_wave_actions(cls, row: Dict[str, Any]) -> List[str]:
        family = cls._guest_family(row)
        control_mode = _norm_text(row.get("control_mode", ""))
        actions: List[str] = []
        if control_mode == "ssh":
            actions.extend(["run_help_command", "list_shell_context", "inspect_processes", "open_editor"])
        elif family == "windows_desktop":
            actions.extend(["open_settings", "focus_sidebar", "open_system_dialog", "traverse_menu"])
        elif family in {"linux_desktop", "unix_desktop"}:
            actions.extend(["open_system_settings", "focus_sidebar", "open_terminal", "traverse_menu"])
        elif family == "macos_desktop":
            actions.extend(["open_system_settings", "open_menu_bar", "focus_sidebar", "open_spotlight"])
        else:
            actions.extend(["traverse_menu", "focus_sidebar", "open_dialog"])
        if control_mode in {"rdp", "vnc"}:
            actions.append("stabilize_remote_focus")
        return _dedupe_strings([action.lower() for action in actions], limit=8)

    @classmethod
    def _recommended_traversal_paths(cls, row: Dict[str, Any]) -> List[str]:
        control_mode = _norm_text(row.get("control_mode", ""))
        family = cls._guest_family(row)
        paths: List[str] = []
        if control_mode == "ssh":
            paths.extend(["shell_prompt->help", "shell_prompt->processes", "shell_prompt->workspace"])
        elif family == "windows_desktop":
            paths.extend(["start_menu->settings", "settings_sidebar->system", "menu->dialog"])
        elif family in {"linux_desktop", "unix_desktop"}:
            paths.extend(["app_menu->settings", "sidebar->preferences", "terminal->workspace"])
        elif family == "macos_desktop":
            paths.extend(["menu_bar->settings", "settings_sidebar->general", "window->dialog"])
        else:
            paths.extend(["menu->dialog", "toolbar->list"])
        if control_mode in {"rdp", "vnc"}:
            paths.append("remote_window->dialog")
        return _dedupe_strings(paths, limit=6)

    @classmethod
    def _guest_capability_tags(cls, row: Dict[str, Any]) -> List[str]:
        family = cls._guest_family(row)
        control_mode = _norm_text(row.get("control_mode", ""))
        tags: List[str] = [family, control_mode]
        if control_mode in {"rdp", "vnc", "ssh"}:
            tags.append("remote_attach")
        if control_mode == "provider_console":
            tags.append("provider_console")
        if family == "terminal":
            tags.append("terminal_safe")
        else:
            tags.append("desktop_surface")
        if bool(row.get("enable_learning", False)):
            tags.append("learning_enabled")
        return _dedupe_strings([tag.lower() for tag in tags if tag], limit=8)

    @classmethod
    def _provider_model_readiness(cls, row: Dict[str, Any], *, machine_profile: Dict[str, Any], task: str = "") -> Dict[str, Any]:
        readiness_status = _norm_text(row.get("readiness_status", "")) or "attention"
        control_mode = _norm_text(row.get("control_mode", ""))
        family = cls._guest_family(row)
        remote_endpoint_ready = bool(_clean_text(row.get("remote_endpoint", "")))
        provider_ready = bool(row.get("provider_detected", False))
        providers_summary = (
            dict(machine_profile.get("providers", {}).get("summary", {}))
            if isinstance(machine_profile.get("providers", {}), dict)
            and isinstance(machine_profile.get("providers", {}).get("summary", {}), dict)
            else {}
        )
        verified_provider_count = int(providers_summary.get("verified_count", 0) or 0)
        local_inventory = (
            dict(machine_profile.get("models", {}).get("local_inventory", {}))
            if isinstance(machine_profile.get("models", {}), dict)
            and isinstance(machine_profile.get("models", {}).get("local_inventory", {}), dict)
            else {}
        )
        multimodal_summary = (
            dict(machine_profile.get("multimodal_memory", {}).get("summary", {}))
            if isinstance(machine_profile.get("multimodal_memory", {}), dict)
            and isinstance(machine_profile.get("multimodal_memory", {}).get("summary", {}), dict)
            else {}
        )
        ai_runtime_summary = (
            dict(machine_profile.get("ai_runtime_profile", {}).get("summary", {}))
            if isinstance(machine_profile.get("ai_runtime_profile", {}), dict)
            and isinstance(machine_profile.get("ai_runtime_profile", {}).get("summary", {}), dict)
            else {}
        )
        app_learning_summary = (
            dict(machine_profile.get("app_learning_plan", {}).get("plan", {}).get("summary", {}))
            if isinstance(machine_profile.get("app_learning_plan", {}), dict)
            and isinstance(machine_profile.get("app_learning_plan", {}).get("plan", {}), dict)
            and isinstance(machine_profile.get("app_learning_plan", {}).get("plan", {}).get("summary", {}), dict)
            else {}
        )
        setup_followthrough_memory = (
            dict(machine_profile.get("setup_followthrough_memory", {}))
            if isinstance(machine_profile.get("setup_followthrough_memory", {}), dict)
            else {}
        )
        continuation_memory = (
            dict(machine_profile.get("continuation_memory", {}))
            if isinstance(machine_profile.get("continuation_memory", {}), dict)
            else {}
        )
        recent_setup_verified_provider_names = _dedupe_strings(
            [
                str(item).strip().lower()
                for item in setup_followthrough_memory.get("top_verified_provider_names", [])
                if isinstance(setup_followthrough_memory.get("top_verified_provider_names", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        recent_setup_manual_input_provider_names = _dedupe_strings(
            [
                str(item).strip().lower()
                for item in setup_followthrough_memory.get("top_manual_input_provider_names", [])
                if isinstance(setup_followthrough_memory.get("top_manual_input_provider_names", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        recent_setup_attention_provider_names = _dedupe_strings(
            [
                str(item).strip().lower()
                for item in setup_followthrough_memory.get("top_attention_provider_names", [])
                if isinstance(setup_followthrough_memory.get("top_attention_provider_names", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        recent_setup_model_item_keys = _dedupe_strings(
            [
                str(item).strip()
                for item in setup_followthrough_memory.get("top_selected_model_item_keys", [])
                if isinstance(setup_followthrough_memory.get("top_selected_model_item_keys", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        recent_setup_ai_runtime_codes = _dedupe_strings(
            [
                str(item).strip().lower()
                for item in setup_followthrough_memory.get("top_ai_runtime_setup_action_codes", [])
                if isinstance(setup_followthrough_memory.get("top_ai_runtime_setup_action_codes", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        recent_setup_multimodal_codes = _dedupe_strings(
            [
                str(item).strip().lower()
                for item in setup_followthrough_memory.get("top_multimodal_setup_action_codes", [])
                if isinstance(setup_followthrough_memory.get("top_multimodal_setup_action_codes", []), list)
                and str(item).strip()
            ],
            limit=8,
        )
        app_learning_memory_mission_status_counts = (
            dict(app_learning_summary.get("memory_mission_status_counts", {}))
            if isinstance(app_learning_summary.get("memory_mission_status_counts", {}), dict)
            else {}
        )
        app_learning_top_memory_mission_queries = [
            str(item).strip()
            for item in (
                dict(app_learning_summary.get("top_memory_mission_queries", {})).keys()
                if isinstance(app_learning_summary.get("top_memory_mission_queries", {}), dict)
                else []
            )
            if str(item).strip()
        ][:8]
        app_learning_top_memory_mission_hotkeys = [
            str(item).strip()
            for item in (
                dict(app_learning_summary.get("top_memory_mission_hotkeys", {})).keys()
                if isinstance(app_learning_summary.get("top_memory_mission_hotkeys", {}), dict)
                else []
            )
            if str(item).strip()
        ][:8]
        local_model_count = int(local_inventory.get("count", 0) or 0)
        vision_runtime_available = bool(multimodal_summary.get("vision_runtime_available", False))
        vision_loaded_model_count = int(multimodal_summary.get("vision_loaded_model_count", 0) or 0)
        local_vision_ready = vision_runtime_available and vision_loaded_model_count > 0
        ai_runtime_status = _norm_text(machine_profile.get("ai_runtime_profile", {}).get("status", ""))
        ai_reasoning_ready = bool(ai_runtime_summary.get("reasoning_runtime_ready", False))
        ai_runtime_ready_stack_count = int(ai_runtime_summary.get("ready_stack_count", 0) or 0)
        ai_runtime_blocked_stack_count = int(ai_runtime_summary.get("blocked_stack_count", 0) or 0)
        ai_runtime_action_required_task_count = int(ai_runtime_summary.get("action_required_task_count", 0) or 0)
        multimodal_memory_pressure = int(multimodal_summary.get("vision_memory_app_count", 0) or 0) + int(
            multimodal_summary.get("weird_app_memory_app_count", 0) or 0
        )
        structured_memory_entry_count = int(multimodal_summary.get("knowledge_store_entry_count", 0) or 0)
        structured_memory_control_count = int(multimodal_summary.get("knowledge_store_control_count", 0) or 0)
        structured_memory_command_count = int(multimodal_summary.get("knowledge_store_command_count", 0) or 0)
        structured_memory_vector_count = int(multimodal_summary.get("knowledge_store_vector_count", 0) or 0)
        structured_memory_low_coverage_count = int(multimodal_summary.get("knowledge_low_coverage_app_count", 0) or 0)
        structured_memory_semantic_ready_count = int(multimodal_summary.get("knowledge_semantic_ready_app_count", 0) or 0)
        app_learning_semantic_guided_count = int(app_learning_summary.get("semantic_guided_count", 0) or 0)
        app_learning_semantic_followup_count = int(app_learning_summary.get("semantic_followup_count", 0) or 0)
        recent_setup_followthrough_status = _norm_text(
            setup_followthrough_memory.get("followthrough_status", "")
        ) or "cold"
        recent_setup_followthrough_recommended = bool(
            setup_followthrough_memory.get("followthrough_recommended", False)
        )
        recent_setup_followthrough_required = bool(
            setup_followthrough_memory.get("followthrough_required", False)
        )
        recent_setup_remaining_ready_count = int(
            setup_followthrough_memory.get("setup_execution_remaining_ready_total", 0) or 0
        )
        recent_setup_provider_blocked_count = int(
            setup_followthrough_memory.get("provider_blocked_total", 0) or 0
        )
        recent_setup_followup_count = int(
            setup_followthrough_memory.get("setup_followup_total", 0) or 0
        )
        effective_verified_provider_count = max(verified_provider_count, len(recent_setup_verified_provider_names))
        recent_setup_reason_codes = [
            str(item).strip().lower()
            for item in setup_followthrough_memory.get("reason_codes", [])
            if isinstance(setup_followthrough_memory.get("reason_codes", []), list) and str(item).strip()
        ][:8]
        recent_continuation_status = _norm_text(
            continuation_memory.get("continuation_status", "")
        ) or "cold"
        recent_continuation_recommended = bool(
            continuation_memory.get("continuation_recommended", False)
        )
        recent_continuation_required = bool(
            continuation_memory.get("continuation_required", False)
        )
        recent_continuation_learning_wave_total = int(
            continuation_memory.get("app_learning_continuation_wave_total", 0) or 0
        )
        recent_continuation_vm_wave_total = int(
            continuation_memory.get("vm_prepare_continuation_wave_total", 0) or 0
        )
        recent_continuation_retry_count = int(
            continuation_memory.get("continuation_retry_total", 0) or 0
        )
        recent_continuation_provider_blocked_count = int(
            continuation_memory.get("continuation_provider_blocked_total", 0) or 0
        )
        recent_continuation_setup_followup_count = int(
            continuation_memory.get("continuation_setup_followup_total", 0) or 0
        )
        recent_continuation_memory_followthrough_count = int(
            continuation_memory.get("continuation_memory_followthrough_total", 0) or 0
        )
        recent_continuation_reason_codes = [
            str(item).strip().lower()
            for item in continuation_memory.get("reason_codes", [])
            if isinstance(continuation_memory.get("reason_codes", []), list) and str(item).strip()
        ][:8]
        recent_continuation_top_memory_mission_queries = [
            str(item).strip()
            for item in (
                dict(continuation_memory.get("top_memory_mission_queries", {})).keys()
                if isinstance(continuation_memory.get("top_memory_mission_queries", {}), dict)
                else []
            )
            if str(item).strip()
        ][:8]
        recent_continuation_top_memory_mission_hotkeys = [
            str(item).strip()
            for item in (
                dict(continuation_memory.get("top_memory_mission_hotkeys", {})).keys()
                if isinstance(continuation_memory.get("top_memory_mission_hotkeys", {}), dict)
                else []
            )
            if str(item).strip()
        ][:8]
        if structured_memory_semantic_ready_count > 0 and structured_memory_low_coverage_count <= 0:
            memory_guidance_status = "strong"
        elif (
            structured_memory_semantic_ready_count > 0
            or structured_memory_vector_count > 0
            or app_learning_semantic_guided_count > 0
        ):
            memory_guidance_status = "partial"
        else:
            memory_guidance_status = "cold"
        memory_guidance_reason_codes = _dedupe_strings(
            [
                "semantic_memory_ready" if structured_memory_semantic_ready_count > 0 else "",
                "vector_memory_available" if structured_memory_vector_count > 0 else "",
                "memory_low_coverage_pressure" if structured_memory_low_coverage_count > 0 else "",
                "learning_semantic_guidance_available" if app_learning_semantic_guided_count > 0 else "",
                "learning_semantic_followup_pending" if app_learning_semantic_followup_count > 0 else "",
                "recent_setup_provider_ready" if recent_setup_verified_provider_names else "",
                "recent_setup_model_selection_available" if recent_setup_model_item_keys else "",
            ],
            limit=6,
        )
        expected_route_profile = cls._expected_route_profile(row, task=task)
        expected_model_preference = cls._expected_model_preference(row, task=task)
        runtime_band_preference = cls._runtime_band_preference(row)
        required_tasks = ["control"]
        if family == "terminal":
            required_tasks.append("reasoning")
        else:
            required_tasks.extend(["vision", "reasoning"])
        local_ready_tasks = ["control"]
        if family == "terminal":
            local_ready_tasks.append("reasoning")
        elif local_model_count > 0 or local_vision_ready:
            local_ready_tasks.append("vision")
        setup_followup_codes = list(row.get("blocker_codes", [])) if isinstance(row.get("blocker_codes", []), list) else []
        if control_mode in {"rdp", "vnc", "ssh"} and not remote_endpoint_ready:
            setup_followup_codes.append("register_remote_endpoint")
        if control_mode == "vnc" and not provider_ready:
            setup_followup_codes.append("install_vnc_viewer")
        if control_mode == "rdp" and not remote_endpoint_ready:
            setup_followup_codes.append("configure_rdp_guest")
        if control_mode == "ssh" and not remote_endpoint_ready:
            setup_followup_codes.append("configure_ssh_guest")
        if expected_model_preference in {"hybrid_runtime", "api_vision_runtime"} and not vision_runtime_available:
            setup_followup_codes.append("initialize_local_vision_runtime")
        elif expected_model_preference in {"hybrid_runtime", "api_vision_runtime"} and vision_loaded_model_count <= 0:
            setup_followup_codes.append("warm_local_vision_runtime")
        if (
            expected_model_preference in {"hybrid_runtime", "api_vision_runtime"}
            and local_model_count <= 0
            and effective_verified_provider_count <= 0
        ):
            setup_followup_codes.append("configure_multimodal_runtime")
        if "reasoning" in required_tasks and not ai_reasoning_ready:
            setup_followup_codes.append("warm_local_reasoning_runtime")
        if ai_runtime_blocked_stack_count > 0:
            setup_followup_codes.append("recover_desktop_agent_stack")
        setup_followup_codes = _dedupe_strings(setup_followup_codes, limit=8)

        selected_ai_runtime_band = runtime_band_preference
        ai_route_status = "matched"
        ai_route_fallback_applied = False
        ai_route_reason_codes: List[str] = []
        if readiness_status == "blocked":
            selected_ai_runtime_band = "accessibility"
            ai_route_status = "blocked"
            ai_route_fallback_applied = True
            ai_route_reason_codes.append("readiness_blocked")
        if "reasoning" in required_tasks and not ai_reasoning_ready and selected_ai_runtime_band in {"local", "hybrid"}:
            ai_route_fallback_applied = True
            ai_route_reason_codes.append("ai_reasoning_runtime_gap")
            if selected_ai_runtime_band == "local":
                selected_ai_runtime_band = "hybrid" if effective_verified_provider_count > 0 else "accessibility"
            elif effective_verified_provider_count <= 0:
                selected_ai_runtime_band = "accessibility"
        if "vision" in required_tasks and not local_vision_ready and selected_ai_runtime_band in {"local", "hybrid"}:
            ai_route_fallback_applied = True
            ai_route_reason_codes.append("ai_vision_runtime_gap")
            selected_ai_runtime_band = "api" if effective_verified_provider_count > 0 else "accessibility"
        if ai_runtime_blocked_stack_count > 0:
            ai_route_reason_codes.append("ai_runtime_stack_attention")
            if ai_route_status == "matched":
                ai_route_status = "fallback" if ai_route_fallback_applied else "setup_constrained"
        if recent_setup_followthrough_required:
            ai_route_reason_codes.append("recent_setup_followthrough_required")
            if ai_route_status == "matched":
                ai_route_status = "setup_waiting"
        elif recent_setup_followthrough_recommended:
            ai_route_reason_codes.append("recent_setup_followthrough_recommended")
        if recent_continuation_required:
            ai_route_reason_codes.append("recent_continuation_required")
            if ai_route_status == "matched":
                ai_route_status = "setup_waiting"
        elif recent_continuation_recommended:
            ai_route_reason_codes.append("recent_continuation_recommended")
        if setup_followup_codes and ai_route_status == "matched":
            ai_route_status = "setup_constrained"
        elif ai_route_status == "matched" and ai_route_fallback_applied:
            ai_route_status = "fallback"

        selected_ai_model_preference = "accessibility"
        selected_ai_provider_source = "accessibility_only"
        selected_ai_route_profile = "accessibility_first"
        if selected_ai_runtime_band == "local":
            selected_ai_model_preference = "local_runtime"
            selected_ai_provider_source = "local_runtime"
            selected_ai_route_profile = expected_route_profile or "vm_local_control"
        elif selected_ai_runtime_band == "hybrid":
            selected_ai_model_preference = "hybrid_runtime"
            selected_ai_provider_source = "local_runtime_plus_ocr"
            selected_ai_route_profile = expected_route_profile or "vm_hybrid_control"
        elif selected_ai_runtime_band == "api":
            selected_ai_model_preference = "api_assist"
            selected_ai_provider_source = "api_assist_plus_ocr"
            selected_ai_route_profile = "api_vm_assist"
        if memory_guidance_status == "strong":
            if selected_ai_route_profile == "accessibility_first":
                selected_ai_route_profile = "accessibility_memory_first"
            elif selected_ai_route_profile:
                selected_ai_route_profile = f"memory_guided_{selected_ai_route_profile}"
            ai_route_reason_codes.append("memory_guided_vm_route")
        elif memory_guidance_status == "partial":
            ai_route_reason_codes.append("memory_assisted_vm_route")
        if recent_setup_verified_provider_names:
            ai_route_reason_codes.append("recent_setup_provider_ready")
        if recent_setup_model_item_keys:
            ai_route_reason_codes.append("recent_setup_model_selection_available")
        if recent_setup_ai_runtime_codes and "reasoning" in required_tasks and not ai_reasoning_ready:
            ai_route_reason_codes.append("recent_setup_reasoning_warmup_available")
        if recent_setup_multimodal_codes and "vision" in required_tasks and not local_vision_ready:
            ai_route_reason_codes.append("recent_setup_multimodal_warmup_available")

        selected_ai_reasoning_stack = "desktop_agent" if ai_reasoning_ready else ""
        selected_ai_vision_stack = "perception" if local_vision_ready and family != "terminal" else ""
        selected_ai_memory_stack = "memory" if ai_runtime_ready_stack_count > 0 else ""
        selected_ai_stack_names = _dedupe_strings(
            [selected_ai_reasoning_stack, selected_ai_vision_stack, selected_ai_memory_stack],
            limit=6,
        )
        ai_route_confidence = 0.48
        ai_route_confidence += min(ai_runtime_ready_stack_count, 3) * 0.09
        ai_route_confidence -= min(ai_runtime_blocked_stack_count, 3) * 0.1
        ai_route_confidence -= min(ai_runtime_action_required_task_count, 4) * 0.05
        ai_route_confidence -= 0.05 if ai_route_fallback_applied else 0.0
        ai_route_confidence -= 0.08 if ai_route_status == "setup_constrained" else 0.0
        ai_route_confidence -= 0.18 if ai_route_status == "blocked" else 0.0
        ai_route_confidence -= 0.04 if structured_memory_low_coverage_count > 0 and family != "terminal" else 0.0
        ai_route_confidence += 0.05 if memory_guidance_status == "strong" else 0.02 if memory_guidance_status == "partial" else 0.0
        ai_route_confidence = max(0.05, min(round(ai_route_confidence, 2), 0.98))
        memory_route_guidance = cls._memory_route_guidance_summary(
            route_profile=selected_ai_route_profile,
            memory_guidance_status=memory_guidance_status,
            reason_codes=ai_route_reason_codes + memory_guidance_reason_codes,
        )

        if readiness_status == "blocked":
            execution_mode = "blocked"
            route_resolution_status = "blocked"
        elif control_mode == "ssh" and remote_endpoint_ready:
            execution_mode = "remote_ready"
            route_resolution_status = "remote_attach"
        elif control_mode in {"rdp", "vnc"} and remote_endpoint_ready:
            execution_mode = "hybrid_ready"
            route_resolution_status = "remote_attach"
        elif provider_ready and control_mode == "provider_console":
            execution_mode = "hybrid_ready" if family != "terminal" else "local_ready"
            route_resolution_status = "matched"
        elif setup_followup_codes:
            execution_mode = "degraded"
            route_resolution_status = "setup_constrained"
        else:
            execution_mode = "attention"
            route_resolution_status = "fallback"
        remediation_kind = cls._vm_remediation_kind(setup_followup_codes)
        return {
            "readiness_status": readiness_status,
            "required_tasks": _dedupe_strings(required_tasks, limit=6),
            "local_ready_tasks": _dedupe_strings(local_ready_tasks, limit=6),
            "provider_ready": provider_ready,
            "verified_provider_count": effective_verified_provider_count,
            "local_model_count": local_model_count,
            "vision_runtime_available": vision_runtime_available,
            "vision_loaded_model_count": vision_loaded_model_count,
            "ai_runtime_status": ai_runtime_status or "unknown",
            "ai_runtime_ready_stack_count": ai_runtime_ready_stack_count,
            "ai_runtime_blocked_stack_count": ai_runtime_blocked_stack_count,
            "ai_runtime_action_required_task_count": ai_runtime_action_required_task_count,
            "ai_reasoning_ready": ai_reasoning_ready,
            "ai_route_status": ai_route_status,
            "ai_route_fallback_applied": ai_route_fallback_applied,
            "ai_route_confidence": ai_route_confidence,
            "selected_ai_runtime_band": selected_ai_runtime_band,
            "selected_ai_route_profile": selected_ai_route_profile,
            "selected_ai_model_preference": selected_ai_model_preference,
            "selected_ai_provider_source": selected_ai_provider_source,
            "selected_ai_reasoning_stack": selected_ai_reasoning_stack,
            "selected_ai_vision_stack": selected_ai_vision_stack,
            "selected_ai_memory_stack": selected_ai_memory_stack,
            "selected_ai_stack_names": selected_ai_stack_names,
            "ai_route_reason_codes": _dedupe_strings(ai_route_reason_codes, limit=10),
            "multimodal_memory_pressure": multimodal_memory_pressure,
            "structured_memory_entry_count": structured_memory_entry_count,
            "structured_memory_control_count": structured_memory_control_count,
            "structured_memory_command_count": structured_memory_command_count,
            "structured_memory_vector_count": structured_memory_vector_count,
            "structured_memory_low_coverage_count": structured_memory_low_coverage_count,
            "structured_memory_semantic_ready_count": structured_memory_semantic_ready_count,
            "app_learning_semantic_guided_count": app_learning_semantic_guided_count,
            "app_learning_semantic_followup_count": app_learning_semantic_followup_count,
            "recent_setup_followthrough_status": recent_setup_followthrough_status,
            "recent_setup_followthrough_recommended": recent_setup_followthrough_recommended,
            "recent_setup_followthrough_required": recent_setup_followthrough_required,
            "recent_setup_remaining_ready_count": recent_setup_remaining_ready_count,
            "recent_setup_provider_blocked_count": recent_setup_provider_blocked_count,
            "recent_setup_followup_count": recent_setup_followup_count,
            "recent_setup_reason_codes": recent_setup_reason_codes,
            "recent_setup_verified_provider_names": recent_setup_verified_provider_names,
            "recent_setup_manual_input_provider_names": recent_setup_manual_input_provider_names,
            "recent_setup_attention_provider_names": recent_setup_attention_provider_names,
            "recent_setup_model_item_keys": recent_setup_model_item_keys,
            "recent_setup_ai_runtime_setup_codes": recent_setup_ai_runtime_codes,
            "recent_setup_multimodal_setup_codes": recent_setup_multimodal_codes,
            "recent_continuation_status": recent_continuation_status,
            "recent_continuation_recommended": recent_continuation_recommended,
            "recent_continuation_required": recent_continuation_required,
            "recent_continuation_learning_wave_total": recent_continuation_learning_wave_total,
            "recent_continuation_vm_wave_total": recent_continuation_vm_wave_total,
            "recent_continuation_retry_count": recent_continuation_retry_count,
            "recent_continuation_provider_blocked_count": recent_continuation_provider_blocked_count,
            "recent_continuation_setup_followup_count": recent_continuation_setup_followup_count,
            "recent_continuation_memory_followthrough_count": recent_continuation_memory_followthrough_count,
            "recent_continuation_reason_codes": recent_continuation_reason_codes,
            "recent_continuation_top_memory_mission_queries": recent_continuation_top_memory_mission_queries,
            "recent_continuation_top_memory_mission_hotkeys": recent_continuation_top_memory_mission_hotkeys,
            "app_learning_memory_mission_status_counts": app_learning_memory_mission_status_counts,
            "app_learning_top_memory_mission_queries": app_learning_top_memory_mission_queries,
            "app_learning_top_memory_mission_hotkeys": app_learning_top_memory_mission_hotkeys,
            "memory_guidance_status": memory_guidance_status,
            "memory_guidance_reason_codes": memory_guidance_reason_codes,
            "memory_guided_route": bool(memory_route_guidance.get("memory_guided_route", False)),
            "memory_assisted_route": bool(memory_route_guidance.get("memory_assisted_route", False)),
            "memory_route_alignment_status": str(
                memory_route_guidance.get("memory_route_alignment_status", "") or ""
            ).strip().lower(),
            "memory_route_reason_codes": list(memory_route_guidance.get("memory_route_reason_codes", []))
            if isinstance(memory_route_guidance.get("memory_route_reason_codes", []), list)
            else [],
            "remote_endpoint_ready": remote_endpoint_ready,
            "execution_mode": execution_mode,
            "runtime_band_preference": runtime_band_preference,
            "expected_route_profile": expected_route_profile,
            "expected_model_preference": expected_model_preference,
            "route_resolution_status": route_resolution_status,
            "setup_followup_codes": setup_followup_codes,
            "remediation_kind": remediation_kind,
        }

    @staticmethod
    def _vm_remediation_kind(setup_followup_codes: Iterable[Any]) -> str:
        codes = [_norm_text(item) for item in setup_followup_codes]
        if any(code == "virtualization_disabled" for code in codes):
            return "enable_virtualization"
        if any(code == "provider_not_detected" for code in codes):
            return "install_provider"
        if any(code == "register_remote_endpoint" for code in codes):
            return "register_remote_endpoint"
        if any(code in {"configure_rdp_guest", "configure_ssh_guest", "install_vnc_viewer"} for code in codes):
            return "configure_remote_attach"
        if any(code in {"configure_multimodal_runtime", "initialize_local_vision_runtime", "warm_local_vision_runtime"} for code in codes):
            return "configure_runtime"
        return "observe"

    @classmethod
    def _plan_guest_target(
        cls,
        row: Dict[str, Any],
        *,
        task: str,
        query: str,
        machine_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        base = dict(row)
        readiness_status = _norm_text(base.get("readiness_status", "")) or "attention"
        reason_codes: List[str] = list(base.get("reason_codes", [])) if isinstance(base.get("reason_codes", []), list) else []
        if readiness_status == "ready":
            reason_codes.append("ready_provider")
        else:
            reason_codes.append("needs_followthrough")
        if bool(base.get("enable_learning", False)):
            reason_codes.append("learning_enabled")
        if query and (
            query in _norm_text(base.get("guest_name", ""))
            or query in _norm_text(base.get("guest_os", ""))
            or query in _norm_text(base.get("provider_label", ""))
        ):
            reason_codes.append("query_match")
        if task and task in _norm_text(base.get("guest_os", "")):
            reason_codes.append("task_match")
        provider_model_readiness = cls._provider_model_readiness(base, machine_profile=machine_profile, task=task)
        execution_mode = _norm_text(provider_model_readiness.get("execution_mode", "")) or "attention"
        priority_score = 120 if readiness_status == "ready" else 88 if readiness_status == "attention" else 42
        if execution_mode in {"hybrid_ready", "remote_ready", "local_ready"}:
            priority_score += 14
        elif execution_mode == "degraded":
            priority_score -= 6
        if query and "query_match" in [_norm_text(item) for item in reason_codes]:
            priority_score += 18
        if task and "task_match" in [_norm_text(item) for item in reason_codes]:
            priority_score += 8
        remediation_kind = _norm_text(provider_model_readiness.get("remediation_kind", "")) or "observe"
        remediation_action_code = str(remediation_kind or "").strip().replace(" ", "_")
        memory_route_alignment_status = _norm_text(provider_model_readiness.get("memory_route_alignment_status", ""))
        memory_followthrough_recommended = memory_route_alignment_status in {"underused", "assisted"}
        recommended_max_surface_waves = (
            6 if memory_route_alignment_status == "underused" else 5 if memory_followthrough_recommended else 4
        )
        recommended_max_probe_controls = (
            5 if memory_route_alignment_status == "underused" else 4 if memory_followthrough_recommended else 3
        )
        memory_mission = cls._guest_memory_mission(
            {**base, "learning_query": cls._guest_learning_query(base)},
            provider_model_readiness=provider_model_readiness,
        )
        return {
            **base,
            "guest_family": cls._guest_family(base),
            "priority_score": priority_score,
            "prepare_priority_band": "high" if priority_score >= 120 else "medium" if priority_score >= 85 else "low",
            "reason_codes": _dedupe_strings(reason_codes, limit=8),
            "auto_prepare_allowed": readiness_status != "blocked",
            "control_strategy": _norm_text(base.get("control_mode", "")) or "provider_console",
            "learning_query": cls._guest_learning_query(base),
            "guest_learning_profile": cls._guest_learning_profile(base, task=task),
            "execution_mode": execution_mode,
            "runtime_band_preference": _norm_text(provider_model_readiness.get("runtime_band_preference", "")),
            "expected_route_profile": _norm_text(provider_model_readiness.get("expected_route_profile", "")),
            "expected_model_preference": _norm_text(provider_model_readiness.get("expected_model_preference", "")),
            "route_resolution_status": _norm_text(provider_model_readiness.get("route_resolution_status", "")),
            "ai_route_status": _norm_text(provider_model_readiness.get("ai_route_status", "")),
            "ai_route_fallback_applied": bool(provider_model_readiness.get("ai_route_fallback_applied", False)),
            "ai_route_confidence": float(provider_model_readiness.get("ai_route_confidence", 0.0) or 0.0),
            "selected_ai_runtime_band": _norm_text(provider_model_readiness.get("selected_ai_runtime_band", "")),
            "selected_ai_route_profile": _norm_text(provider_model_readiness.get("selected_ai_route_profile", "")),
            "selected_ai_model_preference": _norm_text(provider_model_readiness.get("selected_ai_model_preference", "")),
            "selected_ai_provider_source": _norm_text(provider_model_readiness.get("selected_ai_provider_source", "")),
            "memory_guidance_status": _norm_text(provider_model_readiness.get("memory_guidance_status", "")),
            "memory_guidance_reason_codes": list(provider_model_readiness.get("memory_guidance_reason_codes", []))
            if isinstance(provider_model_readiness.get("memory_guidance_reason_codes", []), list)
            else [],
            "memory_guided_route": bool(provider_model_readiness.get("memory_guided_route", False)),
            "memory_assisted_route": bool(provider_model_readiness.get("memory_assisted_route", False)),
            "memory_route_alignment_status": _norm_text(
                provider_model_readiness.get("memory_route_alignment_status", "")
            ),
            "memory_route_reason_codes": list(provider_model_readiness.get("memory_route_reason_codes", []))
            if isinstance(provider_model_readiness.get("memory_route_reason_codes", []), list)
            else [],
            "memory_followthrough_recommended": memory_followthrough_recommended,
            "memory_mission": memory_mission,
            "provider_model_readiness": provider_model_readiness,
            "recommended_traversal_roles": cls._recommended_traversal_roles(base),
            "preferred_wave_actions": cls._preferred_wave_actions(base),
            "recommended_traversal_paths": cls._recommended_traversal_paths(base),
            "recommended_max_surface_waves": recommended_max_surface_waves,
            "recommended_max_probe_controls": recommended_max_probe_controls,
            "capability_tags": cls._guest_capability_tags(base),
            "remediation_kind": remediation_kind,
            "remediation_action_code": remediation_action_code,
        }

    @staticmethod
    def _guest_learning_query(row: Dict[str, Any]) -> str:
        guest_os = _norm_text(row.get("guest_os", ""))
        if "windows" in guest_os:
            return "settings"
        if any(token in guest_os for token in ("linux", "ubuntu", "debian", "fedora", "bsd")):
            return "desktop settings"
        if "mac" in guest_os or "darwin" in guest_os:
            return "system settings"
        return "settings"

    @classmethod
    def _guest_memory_mission(
        cls,
        row: Dict[str, Any],
        *,
        provider_model_readiness: Dict[str, Any],
    ) -> Dict[str, Any]:
        readiness = dict(provider_model_readiness) if isinstance(provider_model_readiness, dict) else {}
        guest_name = _clean_text(row.get("guest_name", ""))
        learning_query = _clean_text(row.get("learning_query", "")) or cls._guest_learning_query(row)
        memory_guidance_status = _norm_text(readiness.get("memory_guidance_status", "")) or "cold"
        alignment_status = _norm_text(readiness.get("memory_route_alignment_status", "")) or "cold"
        mission_status = (
            "strong"
            if alignment_status == "aligned" or bool(readiness.get("memory_guided_route", False))
            else "partial"
            if memory_guidance_status in {"strong", "partial"}
            or int(readiness.get("app_learning_semantic_guided_count", 0) or 0) > 0
            or int(readiness.get("structured_memory_semantic_ready_count", 0) or 0) > 0
            else "cold"
        )
        query_hints = _dedupe_strings(
            [
                learning_query,
                *[
                    str(item).strip()
                    for item in readiness.get("recent_continuation_top_memory_mission_queries", [])
                    if isinstance(readiness.get("recent_continuation_top_memory_mission_queries", []), list)
                    and str(item).strip()
                ],
                *[
                    str(item).strip()
                    for item in readiness.get("app_learning_top_memory_mission_queries", [])
                    if isinstance(readiness.get("app_learning_top_memory_mission_queries", []), list)
                    and str(item).strip()
                ],
                guest_name,
                _clean_text(row.get("provider_label", "")),
            ],
            limit=8,
        )
        hotkey_hints = _dedupe_strings(
            [
                *[
                    str(item).strip()
                    for item in readiness.get("recent_continuation_top_memory_mission_hotkeys", [])
                    if isinstance(readiness.get("recent_continuation_top_memory_mission_hotkeys", []), list)
                    and str(item).strip()
                ],
                *[
                    str(item).strip()
                    for item in readiness.get("app_learning_top_memory_mission_hotkeys", [])
                    if isinstance(readiness.get("app_learning_top_memory_mission_hotkeys", []), list)
                    and str(item).strip()
                ],
            ],
            limit=8,
        )
        followthrough_recommended = (
            alignment_status in {"underused", "assisted"}
            or mission_status == "cold"
            or bool(readiness.get("recent_setup_followthrough_recommended", False))
            or bool(readiness.get("recent_setup_followthrough_required", False))
            or bool(readiness.get("recent_continuation_recommended", False))
            or bool(readiness.get("recent_continuation_required", False))
        )
        return {
            "guest_name": guest_name,
            "status": mission_status,
            "seed_query": next((item for item in query_hints if _clean_text(item)), learning_query),
            "query_hints": query_hints,
            "hotkey_hints": hotkey_hints,
            "memory_guidance_status": memory_guidance_status,
            "memory_route_alignment_status": alignment_status,
            "memory_guided_route": bool(readiness.get("memory_guided_route", False)),
            "memory_assisted_route": bool(readiness.get("memory_assisted_route", False)),
            "followthrough_recommended": followthrough_recommended,
            "reason_codes": _dedupe_strings(
                [
                    *[
                        str(item).strip()
                    for item in readiness.get("memory_route_reason_codes", [])
                    if isinstance(readiness.get("memory_route_reason_codes", []), list) and str(item).strip()
                    ],
                    *(
                        ["recent_setup_followthrough_required"]
                        if bool(readiness.get("recent_setup_followthrough_required", False))
                        else []
                    ),
                    *(
                        ["recent_setup_followthrough_recommended"]
                        if bool(readiness.get("recent_setup_followthrough_recommended", False))
                        else []
                    ),
                    *(
                        ["recent_continuation_required"]
                        if bool(readiness.get("recent_continuation_required", False))
                        else []
                    ),
                    *(
                        ["recent_continuation_recommended"]
                        if bool(readiness.get("recent_continuation_recommended", False))
                        else []
                    ),
                    *(["guest_memory_followthrough_recommended"] if followthrough_recommended else []),
                ],
                limit=12,
            ),
        }

    def _record_prepare_status(self, *, target: Dict[str, Any], status: str, source: str) -> None:
        guest_id = _clean_text(target.get("guest_id", ""))
        if not guest_id:
            return
        profiles = self._store.get("guest_profiles", {})
        rows = dict(profiles) if isinstance(profiles, dict) else {}
        current = dict(rows.get(guest_id, {})) if isinstance(rows.get(guest_id, {}), dict) else {}
        if not current:
            return
        current["last_prepared_at"] = _utc_now_iso()
        current["last_prepare_status"] = _norm_text(status)
        current["last_prepare_source"] = _norm_text(source) or "api"
        current["last_prepare_execution_mode"] = _norm_text(target.get("execution_mode", ""))
        current["last_prepare_route_profile"] = _norm_text(target.get("expected_route_profile", ""))
        current["last_prepare_runtime_band"] = _norm_text(target.get("runtime_band_preference", ""))
        current["last_prepare_learning_profile"] = _norm_text(target.get("guest_learning_profile", ""))
        rows[guest_id] = current
        self._store.set("guest_profiles", rows)
