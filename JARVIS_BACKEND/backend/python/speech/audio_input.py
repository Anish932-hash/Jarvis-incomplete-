import sounddevice as sd
import numpy as np
from typing import Callable, Optional


class AudioInput:
    """
    Advanced real-time audio input with filters + normalization.
    """

    def __init__(self, sample_rate=16000):
        self.sample_rate = sample_rate
        self.stream = None
        self.callback: Optional[Callable] = None

    def start_stream(self, callback: Callable):
        """Start real-time microphone capture."""
        self.callback = callback

        def audio_cb(indata, frames, time, status):
            if status:
                print("Audio status:", status)

            audio = indata[:, 0]

            # Normalization
            audio = audio / (np.max(np.abs(audio)) + 1e-5)

            if self.callback:
                self.callback(audio)

        self.stream = sd.InputStream(
            channels=1,
            samplerate=self.sample_rate,
            dtype="float32",
            callback=audio_cb
        )
        self.stream.start()

    def stop(self):
        if self.stream:
            self.stream.stop()
            self.stream.close()