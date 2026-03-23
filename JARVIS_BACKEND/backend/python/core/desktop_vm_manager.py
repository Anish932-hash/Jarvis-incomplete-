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
                "route_resolution_status_counts": self._count_values(selected, "route_resolution_status"),
                "remediation_kind_counts": self._count_values(selected, "remediation_kind"),
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
                "max_surface_waves": 4,
                "probe_controls": True,
            },
            "next_actions": [
                {
                    "id": f"vm_prepare:{row.get('guest_id', '')}",
                    "kind": "prepare_vm_control",
                    "title": f"Prepare VM control for {_clean_text(row.get('guest_name', 'guest'))}",
                    "target": _clean_text(row.get("guest_name", "guest")),
                    "status": "ready" if bool(row.get("auto_prepare_allowed", False)) else "attention",
                    "recommended_action_code": _clean_text(row.get("remediation_action_code", "")),
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
                "capability_tags": list(target.get("capability_tags", []))
                if isinstance(target.get("capability_tags", []), list)
                else [],
                "provider_model_readiness": dict(target.get("provider_model_readiness", {}))
                if isinstance(target.get("provider_model_readiness", {}), dict)
                else {},
                "reason_codes": list(target.get("reason_codes", [])) if isinstance(target.get("reason_codes", []), list) else [],
                "blocker_codes": list(target.get("blocker_codes", [])) if isinstance(target.get("blocker_codes", []), list) else [],
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
        local_model_count = int(local_inventory.get("count", 0) or 0)
        vision_runtime_available = bool(multimodal_summary.get("vision_runtime_available", False))
        vision_loaded_model_count = int(multimodal_summary.get("vision_loaded_model_count", 0) or 0)
        local_vision_ready = vision_runtime_available and vision_loaded_model_count > 0
        multimodal_memory_pressure = int(multimodal_summary.get("vision_memory_app_count", 0) or 0) + int(
            multimodal_summary.get("weird_app_memory_app_count", 0) or 0
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
        if expected_model_preference in {"hybrid_runtime", "api_vision_runtime"} and local_model_count <= 0 and verified_provider_count <= 0:
            setup_followup_codes.append("configure_multimodal_runtime")
        setup_followup_codes = _dedupe_strings(setup_followup_codes, limit=8)
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
            "verified_provider_count": verified_provider_count,
            "local_model_count": local_model_count,
            "vision_runtime_available": vision_runtime_available,
            "vision_loaded_model_count": vision_loaded_model_count,
            "multimodal_memory_pressure": multimodal_memory_pressure,
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
            "provider_model_readiness": provider_model_readiness,
            "recommended_traversal_roles": cls._recommended_traversal_roles(base),
            "preferred_wave_actions": cls._preferred_wave_actions(base),
            "recommended_traversal_paths": cls._recommended_traversal_paths(base),
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
