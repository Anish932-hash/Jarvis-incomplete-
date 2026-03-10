import io
from typing import Any

import numpy as np
import sounddevice as sd


class AudioOutput:
    """
    Audio output handler for PCM / MP3 / WAV with lazy codec import.
    """

    def __init__(self, volume: float = 1.0):
        self.volume = volume

    def play_pcm(self, pcm: np.ndarray, sample_rate: int) -> None:
        scaled = pcm * self.volume
        sd.play(scaled, sample_rate)
        sd.wait()

    def play_file(self, path: str) -> None:
        from pydub import AudioSegment  # type: ignore

        audio = AudioSegment.from_file(path)
        pcm = np.array(audio.get_array_of_samples(), dtype=np.float32)
        pcm = pcm / max(1e-6, np.max(np.abs(pcm)))
        pcm *= self.volume
        sd.play(pcm, audio.frame_rate)
        sd.wait()

    def play_bytes(self, audio_bytes: bytes, format: str = "mp3") -> None:
        from pydub import AudioSegment  # type: ignore

        audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format=format)
        pcm = np.array(audio.get_array_of_samples(), dtype=np.float32)
        pcm = pcm / max(1e-6, np.max(np.abs(pcm)))
        pcm *= self.volume
        sd.play(pcm, audio.frame_rate)
        sd.wait()

    def stop(self) -> None:
        sd.stop()
