import pvporcupine
import sounddevice as sd
import threading
import numpy as np
from typing import Callable, Optional


class WakewordEngine:
    """
    Real-time wakeword ("Hey Jarvis") detector using Porcupine.
    """

    def __init__(self, keyword_path: str, sensitivity: float = 0.6):
        self.porcupine = pvporcupine.create(
            keyword_paths=[keyword_path],
            sensitivities=[sensitivity]
        )
        self.running = False
        self.thread = None
        self.callback: Optional[Callable] = None

    def start(self, callback: Callable):
        """Begin wakeword detection."""
        self.callback = callback
        self.running = True
        self.thread = threading.Thread(target=self._engine_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        try:
            self.porcupine.delete()
        except Exception:
            pass

    def _engine_loop(self):
        """Continuous microphone monitoring."""
        sample_rate = self.porcupine.sample_rate
        frame_length = self.porcupine.frame_length

        with sd.InputStream(channels=1, samplerate=sample_rate, dtype="int16") as stream:
            while self.running:
                try:
                    frame, _ = stream.read(frame_length)
                    samples = np.asarray(frame[:, 0], dtype=np.int16)
                    if self.porcupine.process(samples) >= 0 and self.callback:
                        self.callback()
                except Exception:
                    continue
