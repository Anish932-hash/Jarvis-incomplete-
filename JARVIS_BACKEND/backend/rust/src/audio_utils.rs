use std::path::Path;
use std::time::Duration;

use rodio::{Decoder, OutputStream, Sink, Source};
use serde_json::json;
use std::fs::File;
use std::io::BufReader;

pub struct AudioPlayback {
    _stream: OutputStream,
    sink: Sink,
}

impl AudioPlayback {
    pub fn stop(&self) {
        self.sink.stop();
    }

    pub fn is_empty(&self) -> bool {
        self.sink.empty()
    }

    pub fn set_volume(&self, volume: f32) {
        self.sink.set_volume(volume.clamp(0.0, 4.0));
    }

    pub fn volume(&self) -> f32 {
        self.sink.volume()
    }

    pub fn sleep_until_end(&self) {
        self.sink.sleep_until_end();
    }
}

pub struct AudioUtils;

impl AudioUtils {
    pub fn play_audio(path: &str) -> anyhow::Result<()> {
        let playback = Self::play_audio_nonblocking(path)?;
        playback.sleep_until_end();
        Ok(())
    }

    pub fn play_audio_nonblocking(path: &str) -> anyhow::Result<AudioPlayback> {
        let (stream, stream_handle) = OutputStream::try_default()?;
        let sink = Sink::try_new(&stream_handle)?;

        let file = File::open(path)?;
        let source = Decoder::new(BufReader::new(file))?;

        sink.append(source);
        Ok(AudioPlayback {
            _stream: stream,
            sink,
        })
    }

    pub fn probe_audio_file(path: &str) -> anyhow::Result<serde_json::Value> {
        let file_path = Path::new(path);
        let metadata = std::fs::metadata(file_path)?;
        let extension = file_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_default();

        if extension == "wav" {
            let reader = hound::WavReader::open(file_path)?;
            let spec = reader.spec();
            let samples = reader.duration();
            let duration_s = if spec.sample_rate > 0 {
                samples as f64 / spec.sample_rate as f64
            } else {
                0.0
            };
            return Ok(json!({
                "path": file_path.to_string_lossy().to_string(),
                "bytes": metadata.len(),
                "format": "wav",
                "channels": spec.channels,
                "sample_rate": spec.sample_rate,
                "bits_per_sample": spec.bits_per_sample,
                "duration_s": duration_s,
                "samples": samples,
            }));
        }

        let file = File::open(file_path)?;
        let source = Decoder::new(BufReader::new(file))?;
        let duration = source.total_duration().unwrap_or(Duration::from_secs(0));

        Ok(json!({
            "path": file_path.to_string_lossy().to_string(),
            "bytes": metadata.len(),
            "format": if extension.is_empty() { "unknown" } else { extension.as_str() },
            "channels": source.channels(),
            "sample_rate": source.sample_rate(),
            "duration_s": duration.as_secs_f64(),
        }))
    }
}
