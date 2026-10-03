use std::sync::Arc;
use std::time::Duration;

use kaspa_stratum_bridge::KaspaApi;

/// Answers 200 with the current mode (`merged`, `kas-only` or `native`; see
/// [`KaspaApi::mining_mode`]) when the bridge can issue work, 503 otherwise.
pub(crate) fn spawn_health_check_server(health_port: String, kaspa_api: Arc<KaspaApi>) {
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        if let Ok(listener) = TcpListener::bind(&health_port).await {
            tracing::info!("Health check server started on {}", health_port);
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let kaspa_api = Arc::clone(&kaspa_api);
                    tokio::spawn(async move {
                        let mut buffer = [0; 1024];
                        if stream.read(&mut buffer).await.is_ok() {
                            // A hung node RPC counts as not ready.
                            let mode = tokio::time::timeout(Duration::from_secs(2), kaspa_api.mining_mode()).await.ok().flatten();
                            let (status, body) = match mode {
                                Some(mode) => ("200 OK", mode),
                                None => ("503 Service Unavailable", "unavailable"),
                            };
                            let response = format!(
                                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                    });
                }
            }
        }
    });
}
