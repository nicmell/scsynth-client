//! The bridge against a real scsynth. The test is ignored by default, and
//! passes at once without `SCSYNTH_ADDR`. The README of the bridge says how
//! to run it.

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use scsynth_client::scsynth_osc::{Command, Packet, Reply};
use scsynth_client::{Incoming, decode};
use scsynth_client_bridge::{Config, DEFAULT_ORIGIN, serve};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::ORIGIN;

/// The longest that the test waits for the reply.
const LIMIT: Duration = Duration::from_secs(5);

#[tokio::test]
#[ignore = "needs a scsynth: see the README"]
async fn a_status_gets_its_reply_through_the_bridge() {
    let Ok(scsynth) = env::var("SCSYNTH_ADDR") else {
        return;
    };
    let scsynth: SocketAddr = scsynth.parse().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        listen: addr,
        scsynth,
        origins: vec![DEFAULT_ORIGIN.to_owned()],
    };
    tokio::spawn(serve(listener, config));

    let mut request = format!("ws://{addr}").into_client_request().unwrap();
    let origin = HeaderValue::from_static(DEFAULT_ORIGIN);
    request.headers_mut().insert(ORIGIN, origin);
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut ws, _) = client_async(request, stream).await.unwrap();

    let status = Packet::Message(Command::Status).encode();
    ws.send(Message::binary(status)).await.unwrap();
    let reply = timeout(LIMIT, async {
        while let Some(message) = ws.next().await {
            let Message::Binary(bytes) = message.unwrap() else {
                continue;
            };
            for incoming in decode(&bytes).unwrap() {
                if let Incoming::Reply(Reply::StatusReply(reply)) = incoming {
                    return reply;
                }
            }
        }
        panic!("the websocket ended before the /status.reply");
    })
    .await
    .expect("no /status.reply within the limit");
    assert!(reply.groups >= 1, "no root group: {reply:?}");
    ws.close(None).await.unwrap();
}
