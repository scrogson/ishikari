//! A node module for ishikari-wasm's tests: `run` does what its `op` input says.

use serde_json::{json, Value};

wit_bindgen::generate!({ path: "../../../wit", world: "node" });

use ishikari::node::host::{self, Level, Request};

const VERSION: u32 = if cfg!(feature = "v2") { 2 } else { 1 };

struct Testkit;

impl Guest for Testkit {
    fn spec() -> String {
        json!({
            "name": "test/kit",
            "category": "test",
            "description": format!("ishikari-wasm's test module, v{VERSION}"),
            "input_schema": {
                "op": { "type": "string", "required": true },
                "text": { "type": "string" },
                "url": { "type": "string" }
            },
            "output_schema": {
                "text": { "type": "string" },
                "version": { "type": "number" }
            }
        })
        .to_string()
    }

    fn run(input: String) -> Result<String, String> {
        let input: Value = serde_json::from_str(&input).map_err(|e| e.to_string())?;
        let inputs = &input["inputs"];
        let text = |name: &str| inputs[name].as_str().unwrap_or_default().to_string();
        let output = match inputs["op"].as_str().unwrap_or_default() {
            "shout" => {
                host::log(Level::Info, &format!("shouting {}", text("text")));
                println!("printed by v{VERSION}");
                eprintln!("warned by v{VERSION}");
                json!({ "text": text("text").to_uppercase(), "version": VERSION, "input": input })
            }
            "spin" => {
                let mut n: u64 = 0;
                loop {
                    n = std::hint::black_box(n.wrapping_add(1));
                }
            }
            "hog" => {
                let mut chunks = Vec::new();
                for _ in 0..1024 {
                    chunks.push(std::hint::black_box(vec![1u8; 1024 * 1024]));
                }
                json!({ "held": chunks.len() })
            }
            "fetch" => {
                let response = host::fetch(&Request {
                    method: "GET".into(),
                    url: text("url"),
                    headers: vec![("x-testkit".into(), "1".into())],
                    body: None,
                })?;
                json!({
                    "status": response.status,
                    "body": String::from_utf8_lossy(&response.body),
                })
            }
            "fail" => {
                host::log(Level::Warn, "about to fail");
                eprintln!("failing in v{VERSION}");
                return Err("asked to fail".into());
            }
            "sandbox" => json!({
                "files": std::fs::read_dir("/").is_ok() || std::fs::read_dir(".").is_ok(),
                "env": std::env::vars().count(),
                "args": std::env::args().count(),
                "socket": std::net::TcpStream::connect("127.0.0.1:5432").is_ok(),
            }),
            "json" => return Ok("not json".into()),
            op => return Err(format!("no op {op:?}")),
        };
        Ok(output.to_string())
    }
}

export!(Testkit);
