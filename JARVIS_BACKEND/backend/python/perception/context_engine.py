"""
Context Engine for continuous desktop awareness and proactive assistance.

Monitors:
- Screen changes and UI state
- Active applications and windows
- User activity patterns
- System state and resource usage
- Workflow detection

Provides:
- Real-time context understanding
- Proactive suggestions
- Intent prediction
- Opportunity detection for assistance
"""

import asyncio
import math
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set

from PIL import Image

from backend.python.core.desktop_state import DesktopState
from backend.python.core.episodic_memory import EpisodicMemory
from backend.python.core.runtime_memory import RuntimeMemory
from backend.python.core.telemetry import Telemetry
from backend.python.pc_control.system_monitor import SystemMonitor
from backend.python.pc_control.window_manager import WindowManager
from backend.python.utils.logger import Logger

from .surface_intelligence import SurfaceIntelligenceAnalyzer
from .vision_engine import VisionEngine, VisualContext


class ActivityType(Enum):
    """Classification of user activity."""
    IDLE = "idle"
    TYPING = "typing"
    BROWSING = "browsing"
    CODING = "coding"
    READING = "reading"
    VIDEO_WATCHING = "video_watching"
    GAMING = "gaming"
    MEETING = "meeting"
    DESIGNING = "designing"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


class FocusMode(Enum):
    """User's current focus/interruption tolerance."""
    DEEP_WORK = "deep_work"          # Don't interrupt
    FOCUSED = "focused"                # Minimal interruptions
    NORMAL = "normal"                  # Standard interruptions OK
    AVAILABLE = "available"            # Open to suggestions
    IDLE = "idle"                      # Proactive assistance welcome


@dataclass
class ActivitySnapshot:
    """Snapshot of user activity at a point in time."""
    timestamp: float
    activity_type: ActivityType
    focus_mode: FocusMode
    active_window_title: str
    active_application: str
    visual_context: Optional[VisualContext]
    typing_speed: float  # keys per minute
    mouse_activity: float  # movements per minute
    cpu_usage: float
    memory_usage: float
    screen_changed: bool
    confidence: float
    surface_role: str = ""
    surface_confidence: float = 0.0
    surface_affordances: List[str] = field(default_factory=list)


@dataclass
class WorkflowPattern:
    """Detected recurring workflow pattern."""
    pattern_id: str
    name: str
    steps: List[str]
    frequency: int  # times observed
    last_seen: float
    typical_duration: float
    typical_time_of_day: List[int]  # hours when typically executed
    confidence: float


@dataclass
class ProactiveOpportunity:
    """Opportunity for proactive assistance."""
    opportunity_id: str
    opportunity_type: str
    description: str
    suggested_action: str
    priority: int  # 1-10, higher = more urgent
    confidence: float
    context: Dict[str, Any]
    expires_at: float


class ContextEngine:
    """
    Continuous desktop context monitoring and understanding.
    
    Provides real-time awareness of:
    - What user is doing
    - What applications are active
    - Visual state of screen
    - Activity patterns and workflows
    - Opportunities for assistance
    """

    def __init__(
        self,
        *,
        vision_engine: VisionEngine,
        desktop_state: DesktopState,
        runtime_memory: RuntimeMemory,
        episodic_memory: EpisodicMemory,
        telemetry: Telemetry,
        monitoring_interval: float = 3.0,
        pattern_detection_enabled: bool = True,
        proactive_suggestions_enabled: bool = True,
    ):
        self.log = Logger("ContextEngine").get_logger()
        
        self.vision = vision_engine
        self.desktop_state = desktop_state
        self.runtime_memory = runtime_memory
        self.episodic_memory = episodic_memory
        self.telemetry = telemetry
        
        self.monitoring_interval = max(1.0, monitoring_interval)
        self.pattern_detection_enabled = pattern_detection_enabled
        self.proactive_suggestions_enabled = proactive_suggestions_enabled
        
        self.window_manager = WindowManager()
        self.system_monitor = SystemMonitor()
        
        # Monitoring state
        self._monitoring_active = False
        self._last_screenshot: Optional[Image.Image] = None
        self._last_screenshot_hash: Optional[str] = None
        self._activity_history: Deque[ActivitySnapshot] = deque(maxlen=1000)
        self._detected_patterns: Dict[str, WorkflowPattern] = {}
        self._active_opportunities: Dict[str, ProactiveOpportunity] = {}
        self._opportunity_dedupe_window_s = 90.0
        self._opportunity_recent_fingerprints: Dict[str, float] = {}
        
        # Activity tracking
        self._keystroke_count = 0
        self._mouse_movement_count = 0
        self._last_activity_time = time.time()
        self._current_focus_mode = FocusMode.NORMAL
        self._latest_surface_analysis: Optional[Dict[str, Any]] = None
        self._last_surface_signature = ""
        self._last_surface_analysis_at = 0.0
        self._surface_analysis_cooldown_s = 6.0
        self.surface_intelligence = SurfaceIntelligenceAnalyzer()
        
        # Callbacks for proactive opportunities
        self._opportunity_callbacks: List[Callable] = []
        
        self.log.info("ContextEngine initialized")

    async def start_monitoring(self):
        """Start continuous desktop monitoring loop."""
        if self._monitoring_active:
            self.log.warning("Monitoring already active")
            return
        
        self._monitoring_active = True
        self.log.info("Starting context monitoring...")
        
        asyncio.create_task(self._monitoring_loop())
        asyncio.create_task(self._pattern_detection_loop())
        asyncio.create_task(self._opportunity_cleanup_loop())

    async def stop_monitoring(self):
        """Stop monitoring loop."""
        self._monitoring_active = False
        self.log.info("Context monitoring stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop - captures screen and analyzes context."""
        while self._monitoring_active:
            try:
                snapshot = await self._capture_context_snapshot()
                
                if snapshot:
                    self._activity_history.append(snapshot)
                    await self._process_snapshot(snapshot)
                    
                    # Emit telemetry
                    self.telemetry.emit(
                        "context.snapshot",
                        {
                            "activity": snapshot.activity_type.value,
                            "focus_mode": snapshot.focus_mode.value,
                            "screen_changed": snapshot.screen_changed,
                        },
                    )
                
                await asyncio.sleep(self.monitoring_interval)
            
            except Exception as exc:
                self.log.error(f"Monitoring loop error: {exc}")
                await asyncio.sleep(self.monitoring_interval * 2)

    async def _capture_context_snapshot(self) -> Optional[ActivitySnapshot]:
        """Capture current context snapshot."""
        try:
            # Get active window info
            get_active = getattr(self.window_manager, "get_active_window", None)
            if callable(get_active):
                active_window = get_active()
            else:
                active_window = self.window_manager.active_window()
            window_title = active_window.get("title", "") if active_window else ""
            app_name = str(active_window.get("app_name", "") or "").strip() if isinstance(active_window, dict) else ""
            if not app_name:
                app_name = self._extract_app_name(window_title)
            
            # Get system metrics
            metrics = self.system_monitor.all_metrics()
            cpu_usage = float(metrics.get("cpu", 0.0) if isinstance(metrics, dict) else 0.0)
            memory_payload = metrics.get("memory", {}) if isinstance(metrics, dict) else {}
            memory_usage = float(memory_payload.get("percent", 0.0)) if isinstance(memory_payload, dict) else 0.0
            
            # Capture screen and check for changes
            screenshot = self.vision.capture_screen()
            screen_changed = False
            visual_context = None
            
            if self._last_screenshot is not None:
                comparison = self.vision.compare_screens(self._last_screenshot, screenshot)
                screen_changed = comparison.get("changed", False)
                change_pct = comparison.get("change_percentage", 0.0)
                
                # Only analyze vision if significant change
                if change_pct > 5.0:
                    visual_context = self.vision.analyze_screen_context(
                        screenshot,
                        detect_objects=False,  # Too expensive for continuous monitoring
                        segment_ui=True,
                        generate_summary=False,
                    )
                    visual_context.active_application = app_name
            else:
                screen_changed = True
            
            self._last_screenshot = screenshot
            
            # Classify activity
            activity_type = self._classify_activity(
                app_name,
                window_title,
                screen_changed,
                cpu_usage,
            )
            
            # Determine focus mode
            focus_mode = self._determine_focus_mode(activity_type, time.time())
            
            # Calculate activity metrics
            typing_speed = self._calculate_typing_speed()
            mouse_activity = self._calculate_mouse_activity()
            surface_analysis = self._refresh_surface_analysis(
                active_window=active_window if isinstance(active_window, dict) else {},
                visual_context=visual_context,
                force=screen_changed,
            )
            
            timestamp = time.time()
            
            return ActivitySnapshot(
                timestamp=timestamp,
                activity_type=activity_type,
                focus_mode=focus_mode,
                active_window_title=window_title,
                active_application=app_name,
                visual_context=visual_context,
                typing_speed=typing_speed,
                mouse_activity=mouse_activity,
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                screen_changed=screen_changed,
                confidence=0.8,
                surface_role=str(surface_analysis.get("surface_role", "") or ""),
                surface_confidence=float(surface_analysis.get("grounding_confidence", 0.0) or 0.0),
                surface_affordances=[
                    str(item).strip()
                    for item in surface_analysis.get("affordances", [])
                    if str(item).strip()
                ],
            )
        
        except Exception as exc:
            self.log.error(f"Failed to capture context snapshot: {exc}")
            return None

    def _refresh_surface_analysis(
        self,
        *,
        active_window: Dict[str, Any],
        visual_context: Optional[VisualContext],
        force: bool = False,
        query: str = "",
    ) -> Dict[str, Any]:
        now = time.time()
        signature = str(active_window.get("window_signature", "") or "").strip()
        should_refresh = force or not self._latest_surface_analysis
        if signature and signature != self._last_surface_signature:
            should_refresh = True
        if not should_refresh and (now - self._last_surface_analysis_at) >= self._surface_analysis_cooldown_s:
            should_refresh = True
        if not should_refresh and isinstance(self._latest_surface_analysis, dict):
            return dict(self._latest_surface_analysis)

        analysis = self._analyze_surface(
            active_window=active_window,
            visual_context=visual_context,
            query=query,
        )
        if isinstance(analysis, dict) and analysis:
            self._latest_surface_analysis = dict(analysis)
            self._last_surface_signature = signature or str(analysis.get("window_signature", "") or "")
            self._last_surface_analysis_at = now
            return dict(analysis)
        return dict(self._latest_surface_analysis or {})

    def _analyze_surface(
        self,
        *,
        active_window: Dict[str, Any],
        visual_context: Optional[VisualContext],
        query: str = "",
        max_elements: int = 180,
    ) -> Dict[str, Any]:
        summary: Dict[str, Any]
        try:
            from backend.python.tools.accessibility_tools import AccessibilityTools

            summary = AccessibilityTools.surface_summary(
                window_title=str(active_window.get("title", "") or ""),
                query=query,
                max_elements=max_elements,
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"Surface accessibility analysis failed: {exc}")
            summary = {"status": "error", "message": str(exc), "surface_flags": {}, "recommended_actions": []}

        try:
            return self.surface_intelligence.analyze(
                window=active_window,
                surface_summary=summary,
                visual_context=visual_context,
                query=query,
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"Surface intelligence analysis failed: {exc}")
            return {
                "window_signature": str(active_window.get("window_signature", "") or ""),
                "surface_role": "unknown",
                "interaction_mode": "content_review",
                "grounding_confidence": 0.0,
                "affordances": [],
                "recovery_hints": [],
                "risk_flags": [],
                "query_resolution": None,
                "source_signals": {"window": active_window, "accessibility": summary},
            }

    def get_surface_summary(
        self,
        *,
        query: str = "",
        include_visual: bool = False,
        max_elements: int = 180,
    ) -> Dict[str, Any]:
        """Get a fused surface analysis for the active window."""
        get_active = getattr(self.window_manager, "get_active_window", None)
        if callable(get_active):
            active_window = get_active()
        else:
            active_window = self.window_manager.active_window()
        active_window = active_window if isinstance(active_window, dict) else {}

        visual_context: Optional[VisualContext] = None
        if include_visual:
            try:
                screenshot = self._last_screenshot or self.vision.capture_screen()
                visual_context = self.vision.analyze_screen_context(
                    screenshot,
                    detect_objects=False,
                    segment_ui=True,
                    generate_summary=False,
                )
            except Exception as exc:  # noqa: BLE001
                self.log.warning(f"Visual surface analysis failed: {exc}")
                visual_context = None

        analysis = self._analyze_surface(
            active_window=active_window,
            visual_context=visual_context,
            query=query,
            max_elements=max_elements,
        )
        self._latest_surface_analysis = dict(analysis)
        self._last_surface_signature = str(active_window.get("window_signature", "") or "")
        self._last_surface_analysis_at = time.time()
        return analysis

    def _extract_app_name(self, window_title: str) -> str:
        """Extract application name from window title."""
        # Common patterns
        title_lower = window_title.lower()
        
        if "visual studio code" in title_lower or "vscode" in title_lower:
            return "vscode"
        elif "chrome" in title_lower:
            return "chrome"
        elif "firefox" in title_lower:
            return "firefox"
        elif "terminal" in title_lower or "powershell" in title_lower or "cmd" in title_lower:
            return "terminal"
        elif "discord" in title_lower:
            return "discord"
        elif "slack" in title_lower:
            return "slack"
        elif "teams" in title_lower:
            return "teams"
        elif "zoom" in title_lower:
            return "zoom"
        elif "spotify" in title_lower:
            return "spotify"
        elif "excel" in title_lower:
            return "excel"
        elif "word" in title_lower:
            return "word"
        elif "powerpoint" in title_lower:
            return "powerpoint"
        elif "notepad" in title_lower:
            return "notepad"
        
        # Extract from title (usually "Document - AppName" format)
        if " - " in window_title:
            return window_title.split(" - ")[-1].lower()
        
        return "unknown"

    def _classify_activity(
        self,
        app_name: str,
        window_title: str,
        screen_changed: bool,
        cpu_usage: float,
    ) -> ActivityType:
        """Classify current user activity based on context signals."""
        # Check for idle first
        time_since_activity = time.time() - self._last_activity_time
        if time_since_activity > 300:  # 5 minutes
            return ActivityType.IDLE
        
        # Application-based classification
        if app_name in {"vscode", "pycharm", "intellij", "sublime"}:
            return ActivityType.CODING
        elif app_name in {"chrome", "firefox", "edge", "brave"}:
            return ActivityType.BROWSING
        elif app_name in {"terminal", "powershell", "cmd", "iterm"}:
            return ActivityType.TERMINAL
        elif app_name in {"zoom", "teams", "meet", "skype"}:
            return ActivityType.MEETING
        elif app_name in {"figma", "photoshop", "illustrator", "canva"}:
            return ActivityType.DESIGNING
        elif app_name in {"spotify", "vlc", "youtube"}:
            return ActivityType.VIDEO_WATCHING
        elif cpu_usage > 50 and screen_changed:
            return ActivityType.GAMING
        
        # Fallback based on activity patterns
        if self._calculate_typing_speed() > 60:  # High typing speed
            return ActivityType.TYPING
        elif not screen_changed:
            return ActivityType.READING
        
        return ActivityType.UNKNOWN

    def _determine_focus_mode(self, activity: ActivityType, current_time: float) -> FocusMode:
        """Determine user's current focus mode and interruption tolerance."""
        time_since_activity = current_time - self._last_activity_time
        
        # Idle
        if time_since_activity > 180:
            return FocusMode.IDLE
        
        # Deep work activities
        if activity in {ActivityType.CODING, ActivityType.DESIGNING}:
            # Check if sustained activity
            recent_activities = [s.activity_type for s in list(self._activity_history)[-20:]]
            if recent_activities.count(activity) >= 15:  # 75% same activity
                return FocusMode.DEEP_WORK
            return FocusMode.FOCUSED
        
        # Meeting - don't interrupt
        if activity == ActivityType.MEETING:
            return FocusMode.DEEP_WORK
        
        # Available for suggestions
        if activity in {ActivityType.BROWSING, ActivityType.IDLE}:
            return FocusMode.AVAILABLE
        
        return FocusMode.NORMAL

    def _calculate_typing_speed(self) -> float:
        """Calculate recent typing speed in keys per minute."""
        # This would integrate with Rust input_analyzer
        # For now, return estimate based on activity
        return self._keystroke_count * 2.0  # Simple approximation

    def _calculate_mouse_activity(self) -> float:
        """Calculate mouse movement activity."""
        return self._mouse_movement_count * 1.5

    async def _process_snapshot(self, snapshot: ActivitySnapshot):
        """Process snapshot and generate insights."""
        # Store in desktop state
        self.desktop_state.update({
            "activity": snapshot.activity_type.value,
            "focus_mode": snapshot.focus_mode.value,
            "app": snapshot.active_application,
            "surface_role": snapshot.surface_role,
            "surface_confidence": round(float(snapshot.surface_confidence), 6),
        })
        
        # Check for proactive opportunities
        if self.proactive_suggestions_enabled and snapshot.focus_mode != FocusMode.DEEP_WORK:
            await self._detect_opportunities(snapshot)

    async def _detect_opportunities(self, snapshot: ActivitySnapshot):
        """Detect opportunities for proactive assistance."""
        opportunities = []
        
        # Detect stuck/error states
        if snapshot.visual_context:
            ui_elements = snapshot.visual_context.ui_elements
            
            # Look for error dialogs
            error_indicators = [e for e in ui_elements if "error" in e.element_type or "alert" in e.element_type]
            if error_indicators:
                opportunities.append(ProactiveOpportunity(
                    opportunity_id=f"error_{int(time.time())}",
                    opportunity_type="error_detected",
                    description="Error dialog detected on screen",
                    suggested_action="Offer to investigate error or search for solution",
                    priority=8,
                    confidence=0.85,
                    context={"elements": len(error_indicators)},
                    expires_at=time.time() + 300,
                ))
        
        # Detect repetitive actions
        if self._is_repetitive_workflow(snapshot):
            opportunities.append(ProactiveOpportunity(
                opportunity_id=f"workflow_{int(time.time())}",
                opportunity_type="workflow_automation",
                description="Repetitive workflow detected",
                suggested_action="Suggest creating macro for repeated actions",
                priority=5,
                confidence=0.7,
                context={"activity": snapshot.activity_type.value},
                expires_at=time.time() + 600,
            ))
        
        # Add opportunities and notify callbacks
        for opp in opportunities:
            fingerprint = self._opportunity_fingerprint(opp)
            if not self._should_emit_opportunity(fingerprint=fingerprint, now=time.time()):
                continue
            self._active_opportunities[opp.opportunity_id] = opp
            await self._notify_opportunity(opp)

    def _is_repetitive_workflow(self, snapshot: ActivitySnapshot) -> bool:
        """Check if current activity is part of repetitive workflow."""
        if len(self._activity_history) < 10:
            return False

        recent = list(self._activity_history)[-14:]
        activity_sequence = [s.activity_type.value for s in recent if isinstance(s, ActivitySnapshot)]
        app_sequence = [str(s.active_application or "").strip().lower() for s in recent if isinstance(s, ActivitySnapshot)]

        if not activity_sequence:
            return False
        if len(set(activity_sequence)) <= 3:
            return True
        if self._detect_cycle_pattern(activity_sequence, max_cycle=4, min_repeats=3):
            return True
        entropy = self._activity_entropy(activity_sequence)
        if entropy <= 0.95 and snapshot.activity_type in {
            ActivityType.TYPING,
            ActivityType.BROWSING,
            ActivityType.CODING,
            ActivityType.TERMINAL,
        }:
            return True
        if self._detect_cycle_pattern(app_sequence, max_cycle=3, min_repeats=3):
            return True

        return False

    @staticmethod
    def _activity_entropy(sequence: List[str]) -> float:
        if not sequence:
            return 0.0
        counts = Counter(str(item or "") for item in sequence if str(item or ""))
        total = float(sum(counts.values()) or 1.0)
        entropy = 0.0
        for count in counts.values():
            probability = float(count) / total
            if probability > 0.0:
                entropy -= probability * math.log2(probability)
        return entropy

    @staticmethod
    def _detect_cycle_pattern(sequence: List[str], *, max_cycle: int = 4, min_repeats: int = 3) -> List[str]:
        clean = [str(item or "").strip().lower() for item in sequence if str(item or "").strip()]
        if len(clean) < (min_repeats * 2):
            return []
        bounded_cycle = max(1, min(int(max_cycle), max(1, len(clean) // 2)))
        bounded_repeats = max(2, min(int(min_repeats), len(clean)))
        for cycle_len in range(1, bounded_cycle + 1):
            cycle = clean[-cycle_len:]
            if not cycle:
                continue
            repeats = 1
            cursor = len(clean) - cycle_len
            while cursor - cycle_len >= 0:
                candidate = clean[cursor - cycle_len : cursor]
                if candidate == cycle:
                    repeats += 1
                    cursor -= cycle_len
                else:
                    break
            if repeats >= bounded_repeats:
                return list(cycle)
        return []

    async def _pattern_detection_loop(self):
        """Background loop for detecting recurring workflow patterns."""
        if not self.pattern_detection_enabled:
            return
        
        while self._monitoring_active:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                if len(self._activity_history) > 50:
                    patterns = self._mine_workflow_patterns()
                    
                    for pattern in patterns:
                        existing = self._detected_patterns.get(pattern.pattern_id)
                        if existing:
                            existing.frequency += 1
                            existing.last_seen = time.time()
                        else:
                            self._detected_patterns[pattern.pattern_id] = pattern
                            self.log.info(f"New workflow pattern detected: {pattern.name}")
            
            except Exception as exc:
                self.log.error(f"Pattern detection error: {exc}")

    def _mine_workflow_patterns(self) -> List[WorkflowPattern]:
        """Mine recurring workflow patterns from activity history."""
        patterns = []
        
        # Simple pattern: sequences of 3+ activities that repeat
        activities = [s.activity_type.value for s in self._activity_history]
        
        # Sliding window to find sequences
        for window_size in [3, 4, 5]:
            sequences = {}
            for i in range(len(activities) - window_size):
                sequence = tuple(activities[i:i+window_size])
                sequences[sequence] = sequences.get(sequence, 0) + 1
            
            # Find frequent sequences (appeared 3+ times)
            for sequence, count in sequences.items():
                if count >= 3:
                    pattern_id = f"pattern_{hash(sequence) % 10000}"
                    patterns.append(WorkflowPattern(
                        pattern_id=pattern_id,
                        name=f"Workflow: {' → '.join(sequence)}",
                        steps=list(sequence),
                        frequency=count,
                        last_seen=time.time(),
                        typical_duration=window_size * self.monitoring_interval,
                        typical_time_of_day=[datetime.now().hour],
                        confidence=min(0.9, count / 10.0),
                    ))
        
        return patterns

    async def _opportunity_cleanup_loop(self):
        """Clean up expired opportunities."""
        while self._monitoring_active:
            try:
                await asyncio.sleep(60)
                
                current_time = time.time()
                expired = [
                    opp_id for opp_id, opp in self._active_opportunities.items()
                    if opp.expires_at < current_time
                ]
                
                for opp_id in expired:
                    del self._active_opportunities[opp_id]
                stale_fingerprints = [
                    key
                    for key, expires_at in self._opportunity_recent_fingerprints.items()
                    if float(expires_at) <= current_time
                ]
                for key in stale_fingerprints:
                    self._opportunity_recent_fingerprints.pop(key, None)
            
            except Exception as exc:
                self.log.error(f"Opportunity cleanup error: {exc}")

    async def _notify_opportunity(self, opportunity: ProactiveOpportunity):
        """Notify registered callbacks about new opportunity."""
        for callback in self._opportunity_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(opportunity)
                else:
                    callback(opportunity)
            except Exception as exc:
                self.log.error(f"Opportunity callback error: {exc}")

    def register_opportunity_callback(self, callback: Callable):
        """Register callback to receive proactive opportunities."""
        self._opportunity_callbacks.append(callback)

    def get_current_context(self) -> Optional[ActivitySnapshot]:
        """Get most recent activity snapshot."""
        return self._activity_history[-1] if self._activity_history else None

    def get_activity_summary(self, duration_minutes: int = 60) -> Dict[str, Any]:
        """Get summary of recent activity."""
        cutoff_time = time.time() - (duration_minutes * 60)
        recent = [s for s in self._activity_history if s.timestamp > cutoff_time]
        
        if not recent:
            return {"status": "no_data"}
        
        # Count activities
        activity_counts = {}
        focus_counts = {}
        for snapshot in recent:
            activity = snapshot.activity_type.value
            activity_counts[activity] = activity_counts.get(activity, 0) + 1
            focus_name = snapshot.focus_mode.value
            focus_counts[focus_name] = focus_counts.get(focus_name, 0) + 1
        surface_counts = Counter(
            str(snapshot.surface_role or "").strip().lower()
            for snapshot in recent
            if str(snapshot.surface_role or "").strip()
        )

        # Calculate averages
        avg_typing_speed = sum(s.typing_speed for s in recent) / len(recent)
        avg_cpu = sum(s.cpu_usage for s in recent) / len(recent)
        avg_mouse = sum(s.mouse_activity for s in recent) / len(recent)
        trend = self.get_activity_trend(window_minutes=max(5, duration_minutes // 2), baseline_minutes=max(duration_minutes, duration_minutes * 3))

        return {
            "duration_minutes": duration_minutes,
            "snapshot_count": len(recent),
            "activity_distribution": activity_counts,
            "focus_distribution": focus_counts,
            "primary_activity": max(activity_counts.items(), key=lambda x: x[1])[0],
            "avg_typing_speed": round(avg_typing_speed, 1),
            "avg_mouse_activity": round(avg_mouse, 1),
            "avg_cpu_usage": round(avg_cpu, 1),
            "screen_changes": sum(1 for s in recent if s.screen_changed),
            "surface_distribution": dict(surface_counts),
            "primary_surface_role": surface_counts.most_common(1)[0][0] if surface_counts else "",
            "trend": trend,
        }

    def get_activity_trend(self, window_minutes: int = 30, baseline_minutes: int = 180) -> Dict[str, Any]:
        now = time.time()
        window_s = max(60, int(window_minutes) * 60)
        baseline_s = max(window_s + 60, int(baseline_minutes) * 60)

        window_cutoff = now - window_s
        baseline_cutoff = now - baseline_s
        window_rows = [row for row in self._activity_history if row.timestamp >= window_cutoff]
        baseline_rows = [row for row in self._activity_history if baseline_cutoff <= row.timestamp < window_cutoff]

        if len(window_rows) < 3 or len(baseline_rows) < 3:
            return {
                "status": "insufficient_data",
                "window_minutes": window_minutes,
                "baseline_minutes": baseline_minutes,
                "window_samples": len(window_rows),
                "baseline_samples": len(baseline_rows),
            }

        def _distribution(rows: List[ActivitySnapshot], *, key_fn: Callable[[ActivitySnapshot], str]) -> Dict[str, float]:
            counts: Dict[str, int] = {}
            for row in rows:
                key = key_fn(row)
                counts[key] = counts.get(key, 0) + 1
            total = float(max(1, len(rows)))
            return {key: value / total for key, value in counts.items()}

        def _drift(current: Dict[str, float], baseline: Dict[str, float]) -> float:
            keys = set(current.keys()).union(baseline.keys())
            return sum(abs(float(current.get(key, 0.0)) - float(baseline.get(key, 0.0))) for key in keys) / 2.0

        current_activity = _distribution(window_rows, key_fn=lambda row: row.activity_type.value)
        baseline_activity = _distribution(baseline_rows, key_fn=lambda row: row.activity_type.value)
        current_focus = _distribution(window_rows, key_fn=lambda row: row.focus_mode.value)
        baseline_focus = _distribution(baseline_rows, key_fn=lambda row: row.focus_mode.value)

        activity_drift = _drift(current_activity, baseline_activity)
        focus_drift = _drift(current_focus, baseline_focus)

        def _avg(rows: List[ActivitySnapshot], field_name: str) -> float:
            return sum(float(getattr(row, field_name)) for row in rows) / float(max(1, len(rows)))

        cpu_now = _avg(window_rows, "cpu_usage")
        cpu_base = _avg(baseline_rows, "cpu_usage")
        typing_now = _avg(window_rows, "typing_speed")
        typing_base = _avg(baseline_rows, "typing_speed")

        cpu_drift = abs(cpu_now - cpu_base) / max(1.0, abs(cpu_base) + 1.0)
        typing_drift = abs(typing_now - typing_base) / max(1.0, abs(typing_base) + 1.0)
        change_intensity = sum(1 for row in window_rows if row.screen_changed) / float(max(1, len(window_rows)))

        anomaly_score = max(
            0.0,
            min(
                1.0,
                (0.36 * activity_drift)
                + (0.22 * focus_drift)
                + (0.18 * cpu_drift)
                + (0.14 * typing_drift)
                + (0.10 * change_intensity),
            ),
        )
        mode = "stable"
        if anomaly_score >= 0.7:
            mode = "volatile"
        elif anomaly_score >= 0.4:
            mode = "shifting"
        return {
            "status": "success",
            "window_minutes": window_minutes,
            "baseline_minutes": baseline_minutes,
            "window_samples": len(window_rows),
            "baseline_samples": len(baseline_rows),
            "activity_drift": round(activity_drift, 6),
            "focus_drift": round(focus_drift, 6),
            "cpu_drift": round(cpu_drift, 6),
            "typing_drift": round(typing_drift, 6),
            "change_intensity": round(change_intensity, 6),
            "anomaly_score": round(anomaly_score, 6),
            "mode": mode,
            "activity_distribution_window": current_activity,
            "activity_distribution_baseline": baseline_activity,
        }

    def predict_next_intents(self, *, window: int = 180, min_support: int = 3, top_k: int = 5) -> Dict[str, Any]:
        bounded_window = max(12, min(int(window), 5000))
        bounded_top_k = max(1, min(int(top_k), 20))
        rows = list(self._activity_history)[-bounded_window:]
        if len(rows) < max(6, int(min_support) + 2):
            return {
                "status": "insufficient_data",
                "window": bounded_window,
                "sample_count": len(rows),
                "predictions": [],
            }

        transitions: Dict[str, Counter[str]] = {}
        for prev, curr in zip(rows[:-1], rows[1:]):
            prev_key = str(prev.activity_type.value)
            curr_key = str(curr.activity_type.value)
            if prev_key not in transitions:
                transitions[prev_key] = Counter()
            transitions[prev_key][curr_key] += 1

        current = rows[-1]
        current_key = str(current.activity_type.value)
        direct = transitions.get(current_key, Counter())
        support = int(sum(direct.values()))

        selected = direct
        selected_source = "direct_transition"
        if support < max(1, int(min_support)):
            global_counts: Counter[str] = Counter()
            for counter in transitions.values():
                global_counts.update(counter)
            selected = global_counts
            support = int(sum(selected.values()))
            selected_source = "global_fallback"

        if support <= 0:
            return {
                "status": "insufficient_data",
                "window": bounded_window,
                "sample_count": len(rows),
                "predictions": [],
            }

        ranked = selected.most_common(bounded_top_k)
        action_hints = {
            "coding": ["run_tests", "summarize_changes", "open_terminal"],
            "browsing": ["summarize_page", "extract_links", "capture_notes"],
            "meeting": ["take_notes", "capture_actions", "draft_followup_email"],
            "reading": ["summarize_document", "extract_todos", "bookmark_context"],
            "terminal": ["suggest_command_macro", "capture_output", "monitor_process"],
            "typing": ["grammar_assist", "autofill_template", "contextual_reply"],
            "video_watching": ["capture_timestamp", "summarize_key_points", "create_task_from_content"],
        }
        predictions: List[Dict[str, Any]] = []
        for activity_name, count in ranked:
            probability = float(count) / float(support)
            predictions.append(
                {
                    "activity": activity_name,
                    "probability": round(probability, 6),
                    "support": int(count),
                    "recommended_actions": action_hints.get(activity_name, ["observe_context", "ask_for_confirmation"]),
                }
            )
        return {
            "status": "success",
            "window": bounded_window,
            "sample_count": len(rows),
            "current_activity": current_key,
            "source": selected_source,
            "transition_support": support,
            "predictions": predictions,
        }

    def assistance_contract(
        self,
        *,
        priority: int,
        trend_window_minutes: int = 30,
        trend_baseline_minutes: int = 180,
    ) -> Dict[str, Any]:
        current = self.get_current_context()
        if not current:
            return {
                "status": "insufficient_context",
                "allowed": True,
                "reason": "no_context_snapshot",
                "readiness_score": 0.5,
            }

        bounded_priority = max(1, min(int(priority), 10))
        focus_scores = {
            FocusMode.DEEP_WORK: 0.12,
            FocusMode.FOCUSED: 0.3,
            FocusMode.NORMAL: 0.56,
            FocusMode.AVAILABLE: 0.78,
            FocusMode.IDLE: 0.94,
        }
        base = float(focus_scores.get(current.focus_mode, 0.5))
        trend = self.get_activity_trend(window_minutes=trend_window_minutes, baseline_minutes=trend_baseline_minutes)
        anomaly = 0.0
        trend_mode = "unknown"
        if isinstance(trend, dict) and str(trend.get("status", "")).strip().lower() == "success":
            anomaly = max(0.0, min(float(trend.get("anomaly_score", 0.0) or 0.0), 1.0))
            trend_mode = str(trend.get("mode", "")).strip().lower() or "stable"

        active_opportunities = len(self._active_opportunities)
        priority_boost = (float(bounded_priority) / 10.0) * 0.28
        opportunity_boost = min(0.12, float(active_opportunities) * 0.04)
        anomaly_penalty = anomaly * 0.22

        readiness = base + priority_boost + opportunity_boost - anomaly_penalty
        if current.focus_mode == FocusMode.DEEP_WORK and bounded_priority < 8:
            readiness -= 0.18
        if current.focus_mode == FocusMode.FOCUSED and bounded_priority < 6:
            readiness -= 0.08
        readiness = max(0.0, min(1.0, readiness))

        interrupt_gate = self.should_interrupt(bounded_priority)
        allowed = bool(interrupt_gate and (readiness >= 0.45 or bounded_priority >= 8))

        reasons: List[str] = []
        reasons.append(f"focus_mode={current.focus_mode.value}")
        reasons.append(f"priority={bounded_priority}")
        reasons.append(f"trend_mode={trend_mode}")
        reasons.append(f"active_opportunities={active_opportunities}")
        if anomaly_penalty > 0.0:
            reasons.append(f"anomaly_penalty={round(anomaly_penalty, 4)}")
        return {
            "status": "success",
            "allowed": allowed,
            "priority": bounded_priority,
            "focus_mode": current.focus_mode.value,
            "current_activity": current.activity_type.value,
            "readiness_score": round(readiness, 6),
            "interrupt_gate": bool(interrupt_gate),
            "trend": trend,
            "reasons": reasons,
        }

    def context_brief(self) -> Dict[str, Any]:
        current = self.get_current_context()
        prediction = self.predict_next_intents(window=180, min_support=3, top_k=3)
        trend = self.get_activity_trend(window_minutes=30, baseline_minutes=180)
        summary = {
            "status": "success",
            "monitoring_active": bool(self._monitoring_active),
            "pattern_detection_enabled": bool(self.pattern_detection_enabled),
            "proactive_suggestions_enabled": bool(self.proactive_suggestions_enabled),
            "active_opportunity_count": len(self._active_opportunities),
            "detected_pattern_count": len(self._detected_patterns),
            "trend": trend,
            "prediction": prediction,
        }
        if current:
            summary["current"] = {
                "activity": current.activity_type.value,
                "focus_mode": current.focus_mode.value,
                "application": current.active_application,
                "window_title": current.active_window_title,
                "typing_speed": round(float(current.typing_speed), 3),
                "mouse_activity": round(float(current.mouse_activity), 3),
                "cpu_usage": round(float(current.cpu_usage), 3),
                "memory_usage": round(float(current.memory_usage), 3),
                "screen_changed": bool(current.screen_changed),
            }
        return summary

    def get_detected_patterns(self) -> List[WorkflowPattern]:
        """Get all detected workflow patterns."""
        return list(self._detected_patterns.values())

    def get_active_opportunities(self) -> List[ProactiveOpportunity]:
        """Get all active proactive opportunities."""
        return list(self._active_opportunities.values())

    def _opportunity_fingerprint(self, opportunity: ProactiveOpportunity) -> str:
        context = opportunity.context if isinstance(opportunity.context, dict) else {}
        app = str(context.get("app", context.get("activity", ""))).strip().lower()
        return (
            f"{opportunity.opportunity_type}|"
            f"{opportunity.suggested_action.strip().lower()}|"
            f"{app}"
        )

    def _should_emit_opportunity(self, *, fingerprint: str, now: float) -> bool:
        clean = str(fingerprint or "").strip().lower()
        if not clean:
            return True
        until = float(self._opportunity_recent_fingerprints.get(clean, 0.0) or 0.0)
        if until > now:
            return False
        self._opportunity_recent_fingerprints[clean] = now + self._opportunity_dedupe_window_s
        return True

    def should_interrupt(self, priority: int) -> bool:
        """
        Determine if it's appropriate to interrupt user.
        
        Args:
            priority: Priority level (1-10) of the interruption
            
        Returns:
            True if interruption is allowed
        """
        current_context = self.get_current_context()
        if not current_context:
            return True  # No context, allow
        
        focus_mode = current_context.focus_mode
        
        if focus_mode == FocusMode.DEEP_WORK:
            return priority >= 9  # Only critical interruptions
        elif focus_mode == FocusMode.FOCUSED:
            return priority >= 7  # Important interruptions
        elif focus_mode == FocusMode.NORMAL:
            return priority >= 5  # Standard interruptions
        elif focus_mode == FocusMode.AVAILABLE:
            return priority >= 3  # Most suggestions allowed
        else:  # IDLE
            return True  # All interruptions welcome
