"""
Perception module for JARVIS - provides multi-modal awareness.

Components:
- VisionEngine: Computer vision with YOLO, SAM, CLIP, BLIP
- ContextEngine: Continuous desktop monitoring and activity understanding
"""

from .context_engine import (
    ActivitySnapshot,
    ActivityType,
    ContextEngine,
    FocusMode,
    ProactiveOpportunity,
    WorkflowPattern,
)
from .vision_engine import (
    DetectedObject,
    UIElement,
    VisualContext,
    VisionEngine,
)

__all__ = [
    "VisionEngine",
    "ContextEngine",
    "DetectedObject",
    "UIElement",
    "VisualContext",
    "ActivitySnapshot",
    "ActivityType",
    "FocusMode",
    "WorkflowPattern",
    "ProactiveOpportunity",
]
