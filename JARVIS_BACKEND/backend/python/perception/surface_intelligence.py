from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from backend.python.utils.logger import Logger

from .vision_engine import VisualContext


@dataclass(slots=True)
class SurfaceQueryResolution:
    query: str
    candidate_count: int
    best_candidate_name: str = ""
    best_candidate_type: str = ""
    best_candidate_id: str = ""
    confidence: float = 0.0


@dataclass(slots=True)
class SurfaceIntelligence:
    timestamp: float
    window_signature: str
    app_name: str
    surface_role: str
    interaction_mode: str
    grounding_confidence: float
    affordances: List[str] = field(default_factory=list)
    recovery_hints: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    reasoning: List[str] = field(default_factory=list)
    query_resolution: Optional[SurfaceQueryResolution] = None
    source_signals: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        if self.query_resolution is None:
            payload["query_resolution"] = None
        return payload


class SurfaceIntelligenceAnalyzer:
    """Fuse window, accessibility, and visual signals into actionable desktop grounding."""

    _BROWSER_APPS = {"chrome", "msedge", "firefox", "brave", "opera"}
    _EDITOR_APPS = {"code", "cursor", "devenv", "notepad++", "sublime_text", "idea64", "pycharm64"}
    _TERMINAL_APPS = {"windowsterminal", "wt", "powershell", "pwsh", "cmd", "conhost"}
    _FILE_MANAGER_APPS = {"explorer", "totalcmd64", "doublecmd"}
    _CHAT_APPS = {"slack", "teams", "discord", "whatsapp", "telegram", "outlook"}
    _DANGEROUS_TERMS = {"delete", "remove", "uninstall", "disable", "erase", "reset", "format"}

    def __init__(self) -> None:
        self.log = Logger("SurfaceIntelligence").get_logger()

    def analyze(
        self,
        *,
        window: Optional[Dict[str, Any]],
        surface_summary: Optional[Dict[str, Any]],
        visual_context: Optional[VisualContext] = None,
        query: str = "",
    ) -> Dict[str, Any]:
        window_payload = dict(window or {})
        summary = dict(surface_summary or {})
        flags = summary.get("surface_flags", {}) if isinstance(summary.get("surface_flags", {}), dict) else {}
        query_candidates = summary.get("query_candidates", []) if isinstance(summary.get("query_candidates", []), list) else []
        inventory = summary.get("control_inventory", []) if isinstance(summary.get("control_inventory", []), list) else []
        app_name = str(window_payload.get("app_name", "") or "").strip().lower()
        title = str(window_payload.get("title", "") or "").strip().lower()
        signature = str(window_payload.get("window_signature", "") or "").strip()

        surface_role = self._infer_surface_role(app_name=app_name, title=title, flags=flags)
        interaction_mode = self._infer_interaction_mode(surface_role=surface_role, flags=flags)
        affordances = self._collect_affordances(flags=flags, summary=summary, query_candidates=query_candidates)
        recovery_hints = self._build_recovery_hints(
            surface_role=surface_role,
            flags=flags,
            affordances=affordances,
            query_candidates=query_candidates,
        )
        risk_flags = self._collect_risk_flags(
            surface_role=surface_role,
            app_name=app_name,
            summary=summary,
            inventory=inventory,
        )
        query_resolution = self._resolve_query(query=query, query_candidates=query_candidates)
        grounding_confidence = self._score_confidence(
            window_payload=window_payload,
            summary=summary,
            visual_context=visual_context,
            query_resolution=query_resolution,
        )
        reasoning = self._reasoning_trace(
            surface_role=surface_role,
            interaction_mode=interaction_mode,
            flags=flags,
            app_name=app_name,
            title=title,
            query_resolution=query_resolution,
            risk_flags=risk_flags,
        )

        intelligence = SurfaceIntelligence(
            timestamp=time.time(),
            window_signature=signature or f"{app_name}|{surface_role}",
            app_name=app_name or "unknown",
            surface_role=surface_role,
            interaction_mode=interaction_mode,
            grounding_confidence=grounding_confidence,
            affordances=affordances,
            recovery_hints=recovery_hints,
            risk_flags=risk_flags,
            reasoning=reasoning,
            query_resolution=query_resolution,
            source_signals={
                "window": {
                    "title": window_payload.get("title", ""),
                    "class_name": window_payload.get("class_name", ""),
                    "surface_hints": window_payload.get("surface_hints", {}),
                },
                "accessibility": {
                    "element_count": summary.get("element_count", 0),
                    "surface_flags": flags,
                    "surface_role_candidates": summary.get("surface_role_candidates", []),
                },
                "vision": {
                    "ui_elements": len(visual_context.ui_elements) if visual_context else 0,
                    "detected_objects": len(visual_context.detected_objects) if visual_context else 0,
                    "has_summary": bool(getattr(visual_context, "screen_summary", "") or ""),
                },
            },
        )
        return intelligence.to_dict()

    def _infer_surface_role(
        self,
        *,
        app_name: str,
        title: str,
        flags: Dict[str, Any],
    ) -> str:
        if flags.get("dialog_visible"):
            return "dialog"
        if app_name in self._BROWSER_APPS:
            return "browser"
        if app_name in self._EDITOR_APPS:
            return "editor"
        if app_name in self._TERMINAL_APPS:
            return "terminal"
        if app_name in self._FILE_MANAGER_APPS:
            return "file_manager"
        if app_name in self._CHAT_APPS:
            return "communication"
        if flags.get("settings_surface_visible") or "settings" in title or "control panel" in title:
            return "settings"
        if flags.get("data_table_visible") and flags.get("toolbar_visible"):
            return "operations_console"
        if flags.get("navigation_tree_visible") and flags.get("list_surface_visible"):
            return "navigator"
        if flags.get("form_surface_visible"):
            return "form"
        if flags.get("text_entry_surface_visible"):
            return "editor_like"
        return "content"

    def _infer_interaction_mode(self, *, surface_role: str, flags: Dict[str, Any]) -> str:
        if surface_role == "dialog":
            return "dialog_resolution"
        if surface_role == "settings":
            return "settings_navigation"
        if surface_role in {"navigator", "file_manager"}:
            return "tree_list_navigation"
        if surface_role in {"editor", "editor_like"}:
            return "document_editing"
        if surface_role == "terminal":
            return "command_execution"
        if flags.get("form_surface_visible"):
            return "form_fill"
        if flags.get("data_table_visible"):
            return "table_navigation"
        return "content_review"

    def _collect_affordances(
        self,
        *,
        flags: Dict[str, Any],
        summary: Dict[str, Any],
        query_candidates: List[Dict[str, Any]],
    ) -> List[str]:
        affordances = [str(item).strip() for item in summary.get("recommended_actions", []) if str(item).strip()]
        if query_candidates:
            affordances.append("select_query_target")
            affordances.append("query_target_available")
        if flags.get("scrollable_surface_visible"):
            affordances.append("scroll_search")
        if flags.get("selection_surface_visible"):
            affordances.append("selection_targeting")
        if flags.get("toolbar_visible"):
            affordances.append("toolbar_dispatch")
        deduped: List[str] = []
        for item in affordances:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _build_recovery_hints(
        self,
        *,
        surface_role: str,
        flags: Dict[str, Any],
        affordances: List[str],
        query_candidates: List[Dict[str, Any]],
    ) -> List[str]:
        hints: List[str] = []
        if surface_role == "dialog":
            hints.append("prefer explicit dialog buttons over blind keypress fallbacks")
        if flags.get("navigation_tree_visible") and flags.get("list_surface_visible"):
            hints.append("stabilize focus on the navigation tree before selecting content")
        if flags.get("form_surface_visible"):
            hints.append("verify visible field state before attempting a commit action")
        if "scroll_search" in affordances:
            hints.append("continue with bounded scroll discovery if the requested target is not visible")
        if query_candidates:
            hints.append("use the strongest query candidate before entering exploratory fallback mode")
        if not hints:
            hints.append("re-observe the active surface before branching into unsupported-app recovery")
        return hints

    def _collect_risk_flags(
        self,
        *,
        surface_role: str,
        app_name: str,
        summary: Dict[str, Any],
        inventory: List[Dict[str, Any]],
    ) -> List[str]:
        risks: List[str] = []
        destructive = [str(item).strip().lower() for item in summary.get("destructive_candidates", []) if str(item).strip()]
        if destructive:
            risks.append("destructive_controls_visible")
        if surface_role == "dialog" and destructive:
            risks.append("destructive_dialog_path")
        if app_name in {"taskmgr", "regedit", "mmc", "powershell", "cmd", "pwsh"}:
            risks.append("admin_or_ops_surface")
        names = " ".join(str(row.get("name", "")).strip().lower() for row in inventory)
        if any(token in names for token in ("administrator", "permission", "credential", "password")):
            risks.append("approval_or_credential_surface")
        return risks

    def _resolve_query(
        self,
        *,
        query: str,
        query_candidates: List[Dict[str, Any]],
    ) -> Optional[SurfaceQueryResolution]:
        clean = str(query or "").strip()
        if not clean:
            return None
        if not query_candidates:
            return SurfaceQueryResolution(query=clean, candidate_count=0, confidence=0.0)
        top = query_candidates[0]
        score = float(top.get("match_score", 0.0) or 0.0)
        return SurfaceQueryResolution(
            query=clean,
            candidate_count=len(query_candidates),
            best_candidate_name=str(top.get("name", "") or ""),
            best_candidate_type=str(top.get("control_type", "") or ""),
            best_candidate_id=str(top.get("element_id", "") or ""),
            confidence=max(0.0, min(1.0, score if score > 0.0 else 0.72)),
        )

    def _score_confidence(
        self,
        *,
        window_payload: Dict[str, Any],
        summary: Dict[str, Any],
        visual_context: Optional[VisualContext],
        query_resolution: Optional[SurfaceQueryResolution],
    ) -> float:
        score = 0.18
        if window_payload.get("window_signature"):
            score += 0.16
        if window_payload.get("surface_hints"):
            score += 0.08
        element_count = int(summary.get("element_count", 0) or 0)
        if element_count >= 8:
            score += 0.18
        elif element_count >= 3:
            score += 0.1
        if summary.get("surface_flags"):
            score += 0.14
        if query_resolution is not None:
            score += min(0.18, max(0.02, float(query_resolution.confidence) * 0.2))
        if visual_context is not None:
            if visual_context.ui_elements:
                score += 0.08
            if visual_context.screen_summary:
                score += 0.06
        return round(max(0.0, min(score, 1.0)), 6)

    def _reasoning_trace(
        self,
        *,
        surface_role: str,
        interaction_mode: str,
        flags: Dict[str, Any],
        app_name: str,
        title: str,
        query_resolution: Optional[SurfaceQueryResolution],
        risk_flags: List[str],
    ) -> List[str]:
        reasons = [f"surface_role:{surface_role}", f"interaction_mode:{interaction_mode}"]
        if app_name:
            reasons.append(f"app:{app_name}")
        if title:
            reasons.append(f"title_hint:{title[:80]}")
        for key, value in sorted(flags.items()):
            if value:
                reasons.append(f"flag:{key}")
        if query_resolution and query_resolution.candidate_count:
            reasons.append(
                f"query_match:{query_resolution.best_candidate_name or query_resolution.best_candidate_type}"
            )
        for risk in risk_flags:
            reasons.append(f"risk:{risk}")
        return reasons[:16]
