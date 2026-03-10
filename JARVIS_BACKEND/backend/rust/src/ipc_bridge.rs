use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub struct IpcBridge {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl IpcBridge {
    pub async fn new(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::from_stream(stream))
    }

    pub async fn listen(addr: &str) -> anyhow::Result<TcpListener> {
        let listener = TcpListener::bind(addr).await?;
        Ok(listener)
    }

    pub async fn send(&mut self, json: &Value) -> anyhow::Result<()> {
        let mut packet = serde_json::to_vec(json)?;
        packet.push(b'\n');
        self.writer.write_all(&packet).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub fn from_stream(stream: TcpStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    pub async fn receive_message(&mut self) -> anyhow::Result<Value> {
        let mut line = String::new();
        let bytes = self.reader.read_line(&mut line).await?;
        if bytes == 0 {
            anyhow::bail!("IPC connection closed");
        }
        let json_msg: Value = serde_json::from_str(line.trim())?;
        Ok(json_msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn tcp_bridge_roundtrip_messages() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("server should accept");
            let mut bridge = IpcBridge::from_stream(stream);
            let incoming = bridge
                .receive_message()
                .await
                .expect("server should receive message");
            assert_eq!(incoming.get("event").and_then(Value::as_str), Some("ping"));
            bridge
                .send(&json!({"event":"pong"}))
                .await
                .expect("server should send response");
        });

        let mut client = IpcBridge::new(&addr.to_string())
            .await
            .expect("client should connect");
        client
            .send(&json!({"event":"ping"}))
            .await
            .expect("client should send");
        let reply = client
            .receive_message()
            .await
            .expect("client should receive response");
        assert_eq!(reply.get("event").and_then(Value::as_str), Some("pong"));

        server.await.expect("server task should complete");
    }

    #[tokio::test]
    async fn receive_message_reports_invalid_json() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose local addr");

        let writer = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr)
                .await
                .expect("client should connect");
            stream
                .write_all(b"{invalid-json}\n")
                .await
                .expect("client should write");
        });

        let (stream, _) = listener.accept().await.expect("server should accept");
        let mut bridge = IpcBridge::from_stream(stream);
        let err = bridge
            .receive_message()
            .await
            .expect_err("invalid JSON should produce error");
        let text = err.to_string().to_ascii_lowercase();
        assert!(
            text.contains("expected")
                || text.contains("invalid")
                || text.contains("syntax")
                || text.contains("key")
        );

        writer.await.expect("writer task should complete");
    }
}
