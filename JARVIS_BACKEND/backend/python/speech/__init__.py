"""
Speech package exports are lazy to avoid import-time dependency failures.
"""

from importlib import import_module
from typing import Any

__all__ = [
    "ElevenLabsTTS",
    "LocalTTS",
    "TtsPolicyManager",
    "PlaybackSessionRegistry",
    "LocalNeuralTtsBridge",
    "STTEngine",
    "WakewordEngine",
    "AudioInput",
    "AudioOutput",
]

_MODULE_MAP = {
    "ElevenLabsTTS": "backend.python.speech.elevenlabs_tts",
    "LocalTTS": "backend.python.speech.local_tts",
    "TtsPolicyManager": "backend.python.speech.tts_policy",
    "PlaybackSessionRegistry": "backend.python.speech.playback_session",
    "LocalNeuralTtsBridge": "backend.python.speech.local_tts_bridge",
    "STTEngine": "backend.python.speech.stt_engine",
    "WakewordEngine": "backend.python.speech.wakeword_engine",
    "AudioInput": "backend.python.speech.audio_input",
    "AudioOutput": "backend.python.speech.audio_output",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULE_MAP.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)
