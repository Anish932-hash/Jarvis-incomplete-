use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::mpsc::channel;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct FileAccess;

impl FileAccess {
    pub async fn read_text(path: &str) -> anyhow::Result<String> {
        let mut file = fs::File::open(path).await?;
        let mut buf = String::new();
        file.read_to_string(&mut buf).await?;
        Ok(buf)
    }

    pub async fn write_text(path: &str, data: &str) -> anyhow::Result<()> {
        let mut file = fs::File::create(path).await?;
        file.write_all(data.as_bytes()).await?;
        Ok(())
    }

    pub async fn read_json<T: DeserializeOwned>(path: &str) -> anyhow::Result<T> {
        let text = Self::read_text(path).await?;
        Ok(serde_json::from_str(&text)?)
    }

    pub async fn write_json<T: Serialize>(path: &str, data: &T) -> anyhow::Result<()> {
        let json_string = serde_json::to_string_pretty(data)?;
        Self::write_text(path, &json_string).await
    }

    pub fn watch_directory<F>(path: &str, callback: F) -> anyhow::Result<()>
    where
        F: Fn(Event) + Send + 'static,
    {
        let (tx, rx) = channel();

        let mut watcher: RecommendedWatcher =
            RecommendedWatcher::new(tx, notify::Config::default())?;

        watcher.watch(Path::new(path), RecursiveMode::Recursive)?;

        std::thread::spawn(move || {
            for event in rx {
                if let Ok(ev) = event {
                    callback(ev);
                }
            }
        });

        Ok(())
    }

    pub async fn sha256_of_file(path: &str) -> anyhow::Result<String> {
        let mut file = fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 4096];

        loop {
            let bytes = file.read(&mut buf).await?;
            if bytes == 0 {
                break;
            }
            hasher.update(&buf[..bytes]);
        }

        Ok(format!("{:x}", hasher.finalize()))
    }
}
