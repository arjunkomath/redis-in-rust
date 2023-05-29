use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time;

mod resp;

async fn handle_client(stream: TcpStream, db: Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
    println!("client connected");
    let mut conn = resp::RespConnection::new(stream);

    loop {
        let value = conn.read_value().await?;

        if let Some(value) = value {
            let (command, args) = value.to_command()?;

            let response = match command.to_ascii_lowercase().as_ref() {
                "ping" => resp::Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),

                "set" => {
                    let key = args.first().unwrap().to_string();
                    let value = args.get(1).unwrap().to_string();

                    db.lock().unwrap().insert(key.clone(), value);

                    // Handle expiry
                    let arg = args.get(2);

                    if let Some(arg) = arg {
                        // we don't care about anything other than PX
                        if arg.to_string().to_ascii_lowercase() == "px" {
                            let expiry_in_ms =
                                args.get(3).unwrap().to_string().parse::<u64>().unwrap();

                            let db = db.clone();
                            tokio::spawn(async move {
                                time::sleep(Duration::from_millis(expiry_in_ms)).await;
                                db.lock().unwrap().remove(&key);
                            });
                        }
                    }

                    resp::Value::SimpleString("OK".to_string())
                }

                "get" => {
                    let key = args.first().unwrap().to_string();

                    if let Some(value) = db.lock().unwrap().get(&key) {
                        resp::Value::BulkString(value.to_string())
                    } else {
                        resp::Value::NullValue()
                    }
                }

                _ => resp::Value::Error(format!("command not implemented: {}", command)),
            };

            conn.write_value(response).await?;
        } else {
            println!("client disconnected");
            break;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let incoming = listener.accept().await;
        let db = db.clone();

        match incoming {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_client(stream, db).await.unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
