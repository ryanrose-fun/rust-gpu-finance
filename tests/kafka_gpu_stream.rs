#![cfg(all(feature = "gpu", feature = "streaming"))]
//! Explicit local Kafka + discrete GPU acceptance and sustained-flow experiment.
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
const KEY: &str = "synthetic-kafka-acceptance-key-0123456789";
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn log(root: &Path, name: &str) -> Stdio {
    Stdio::from(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(name))
            .unwrap(),
    )
}
fn api(root: &Path, port: u16, capacity: usize) -> Process {
    let mut c = Command::new(env!("CARGO_BIN_EXE_api"));
    for (k, _) in std::env::vars().filter(|(k, _)| k.starts_with("RECONCILE_")) {
        c.env_remove(k);
    }
    let scoped = root.join("tenant.json").exists();
    c.env("RECONCILE_API_KEY_STDIN", "1").stdin(Stdio::piped());
    if scoped {
        c.env_remove("RECONCILE_API_KEY")
            .env_remove("RECONCILE_API_KEY_STDIN")
            .env("RECONCILE_TENANTS_FILE", root.join("tenant.json"));
    }
    let mut child = c
        .env("RECONCILE_BIND", format!("127.0.0.1:{port}"))
        .env("RECONCILE_GPU_DB_PATH", root.join("gpu.sqlite3"))
        .env("RECONCILE_DB_PATH", root.join("events.sqlite3"))
        .env("RECONCILE_GPU_CAPACITY", capacity.to_string())
        .env("RECONCILE_REQUESTS_PER_SECOND", "10000")
        .env("RECONCILE_GPU_PROFILE", "1")
        .env("RECONCILE_MAX_BODY_BYTES", "16777216")
        .stdout(log(root, "api.out.log"))
        .stderr(log(root, "api.err.log"))
        .spawn()
        .unwrap();
    if !scoped {
        writeln!(child.stdin.take().unwrap(), "{KEY}").unwrap();
    }
    Process(child)
}

#[tokio::test]
#[ignore = "requires discrete GPU; scoped product workflow and restart"]
async fn scoped_product_workflow_survives_restart() {
    let root = std::env::temp_dir().join(format!("gpu-product-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let credentials: Vec<_> = [
        ("admin", "Administrator", KEY.to_string()),
        ("bank", "InternalWriter", "b".repeat(40)),
        ("provider", "ProviderWriter", "p".repeat(40)),
        ("audit", "Auditor", "a".repeat(40)),
        ("investigator", "Investigator", "i".repeat(40)),
    ]
    .into_iter()
    .map(|(id, role, api_key)| json!({"id":id,"role":role,"api_key":api_key}))
    .collect();
    std::fs::write(root.join("tenant.json"),serde_json::to_vec(&json!([{"id":"bank-a","database":root.join("events.sqlite3"),"credentials":credentials}])).unwrap()).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut server = api(&root, port, 100);
    wait_ready(&http, &url).await;
    let event = |source: &str, version: u64, amount: u64| json!({"event_id":format!("{source}-{version}"),"source":source,"version":version,"payment":{"id":"p1","amount_minor":amount,"currency":"GBP","status":"Completed"}});
    let send = |key: String, body: Value| {
        http.post(format!("{url}/v1/gpu/events"))
            .bearer_auth(key)
            .json(&body)
            .send()
    };
    assert_eq!(
        send(
            "p".repeat(40),
            json!({"request_id":"forbidden","events":[event("Internal",1,100)]})
        )
        .await
        .unwrap()
        .status(),
        403
    );
    assert_eq!(
        send(
            "b".repeat(40),
            json!({"request_id":"bank-1","events":[event("Internal",1,100)]})
        )
        .await
        .unwrap()
        .status(),
        200
    );
    let payment: Value = http
        .get(format!("{url}/v1/gpu/payments/p1"))
        .bearer_auth("a".repeat(40))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(payment["awaiting_counterpart"], true);
    assert_eq!(
        send(
            "p".repeat(40),
            json!({"request_id":"provider-1","events":[event("Provider",1,90)]})
        )
        .await
        .unwrap()
        .status(),
        200
    );
    let feed: Value = http
        .get(format!("{url}/v1/gpu/findings?after=0&limit=20"))
        .bearer_auth("a".repeat(40))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let cursor = feed["next_cursor"].as_i64().unwrap();
    assert_eq!(feed["entries"].as_array().unwrap().len(), 2);
    let note = json!({"request_id":"note-1","note":"Provider contacted","acknowledged":true});
    assert_eq!(
        http.post(format!("{url}/v1/gpu/payments/p1/investigation"))
            .bearer_auth("a".repeat(40))
            .json(&note)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    for _ in 0..2 {
        assert_eq!(
            http.post(format!("{url}/v1/gpu/payments/p1/investigation"))
                .bearer_auth("i".repeat(40))
                .json(&note)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    assert_eq!(
        http.post(format!("{url}/v1/gpu/checkpoint"))
            .bearer_auth("a".repeat(40))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        http.get(format!("{url}/v1/gpu/findings"))
            .bearer_auth("other-tenant-key-not-authorized-0123456789")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        send(
            "p".repeat(40),
            json!({"request_id":"provider-2","events":[event("Provider",2,100)]})
        )
        .await
        .unwrap()
        .status(),
        200
    );
    drop(server);
    server = api(&root, port, 100);
    wait_ready(&http, &url).await;
    let feed: Value = http
        .get(format!("{url}/v1/gpu/findings?after={cursor}"))
        .bearer_auth("a".repeat(40))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(feed["entries"][0]["changes"][0]["transition"], "Resolved");
    let notes: Value = http
        .get(format!("{url}/v1/gpu/payments/p1/investigation"))
        .bearer_auth("a".repeat(40))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(notes["entries"].as_array().unwrap().len(), 1);
    let history: Value = http
        .get(format!("{url}/v1/gpu/payments/p1/history"))
        .bearer_auth("a".repeat(40))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history["entries"].as_array().unwrap().len(), 3);
    assert_eq!(
        history["entries"][1]["evidence"][0]["payment"]["amount_minor"],
        90
    );
    println!(
        "Scoped GPU product recovery passed; evidence: {}",
        root.display()
    );
    drop(server);
}
fn stream(root: &Path, args: &[String], label: &str, pause: bool) -> Process {
    let mut c = Command::new(env!("CARGO_BIN_EXE_payment-stream"));
    c.args(args)
        .env_remove("RECONCILE_API_KEY")
        .env_remove("PAYMENT_STREAM_INTERNAL_KEY")
        .env_remove("PAYMENT_STREAM_PROVIDER_KEY")
        .env("PAYMENT_STREAM_KEY_STDIN", "1")
        .stdin(Stdio::piped());
    if pause {
        c.env("PAYMENT_STREAM_TEST_PAUSE_AFTER_ACK_MS", "10000")
            .env("PAYMENT_STREAM_TEST_PAUSE_FILE", root.join("pause"));
    }
    if args[0] == "generate" {
        c.env("PAYMENT_STREAM_START_FILE", root.join("start"));
    }
    let mut child = c
        .stdout(log(root, &format!("{label}.jsonl")))
        .stderr(log(root, &format!("{label}.err.log")))
        .spawn()
        .unwrap();
    if args[0] == "consume" {
        writeln!(child.stdin.take().unwrap(), "{KEY}").unwrap();
    }
    Process(child)
}
fn rows(root: &Path, label: &str) -> Vec<Value> {
    BufReader::new(File::open(root.join(format!("{label}.jsonl"))).unwrap())
        .lines()
        .map_while(Result::ok)
        .filter_map(|s| serde_json::from_str(&s).ok())
        .collect()
}
async fn wait_ready(http: &reqwest::Client, url: &str) {
    let began = Instant::now();
    loop {
        if let Ok(r) = http
            .get(format!("{url}/v1/gpu/health"))
            .bearer_auth(KEY)
            .send()
            .await
            && r.status().is_success()
        {
            return;
        }
        assert!(
            began.elapsed() < Duration::from_secs(120),
            "GPU startup timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
fn pct(mut values: Vec<f64>, p: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1)]
}
async fn wait_consumer(root: &Path, process: &mut Process, expected: usize) {
    let began = Instant::now();
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "Consumer exited before readiness; inspect evidence"
        );
        let log = std::fs::read_to_string(root.join("consumer.jsonl")).unwrap();
        if log
            .lines()
            .filter(|l| l.contains("\"kind\":\"consumer_ready\""))
            .count()
            >= expected
        {
            return;
        }
        assert!(
            began.elapsed() < Duration::from_secs(90),
            "Consumer readiness timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires running Kafka on loopback and a discrete GPU"]
async fn sustained_burst_and_restart() {
    let seconds: u64 = std::env::var("STREAM_TEST_SECONDS")
        .unwrap_or("90".into())
        .parse()
        .unwrap();
    assert!(seconds >= 12);
    let rate: u64 = std::env::var("STREAM_TEST_RATE")
        .unwrap_or("2000".into())
        .parse()
        .unwrap();
    let mode = std::env::var("STREAM_TEST_MODE").unwrap_or("combined".into());
    assert!(["combined", "burst", "restart", "steady"].contains(&mode.as_str()));
    let burst_rate = if mode == "restart" || mode == "steady" {
        rate
    } else {
        rate * 5
    };
    let burst_start = seconds / 3;
    let burst_duration = (seconds / 6).min(10);
    let count = seconds * rate + burst_duration * (burst_rate - rate);
    let run = format!("stream-{}", uuid::Uuid::new_v4().simple());
    let root: PathBuf = std::env::temp_dir().join(&run);
    std::fs::create_dir(&root).unwrap();
    println!("Evidence: {}", root.display());
    let broker = std::env::var("KAFKA_BROKER").unwrap_or("127.0.0.1:19092".into());
    reconcile_cli::streaming::run(
        vec![
            "payment-stream".into(),
            "init".into(),
            broker.clone(),
            run.clone(),
        ],
        None,
    )
    .await
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let url = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut server = api(&root, port, count as usize);
    wait_ready(&http, &url).await;
    let consume = vec![
        "consume".into(),
        broker.clone(),
        run.clone(),
        url.clone(),
        root.join("cursor.sqlite3").to_string_lossy().into_owned(),
    ];
    let mut consumer = stream(&root, &consume, "consumer", true);
    wait_consumer(&root, &mut consumer, 1).await;

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut producers = Vec::new();
    for source in ["Internal", "Provider"] {
        let args = vec![
            "payment-stream".into(),
            "generate".into(),
            broker.clone(),
            run.clone(),
            source.into(),
            seconds.to_string(),
            rate.to_string(),
            burst_start.to_string(),
            burst_duration.to_string(),
            burst_rate.to_string(),
        ];
        let gate = barrier.clone();
        producers.push(tokio::spawn(async move {
            reconcile_cli::streaming::run(args, Some(gate)).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(60), barrier.wait())
        .await
        .unwrap();
    let started = Instant::now();
    let started_ms = chrono::Utc::now().timestamp_millis();
    println!("Both generator tasks ready; synchronized traffic started");
    let mut stopped_at = 0.;
    let mut recovery_s = 0.;
    let mut consumer_recovery_s = 0.;
    if mode == "combined" || mode == "restart" {
        // Let steady and burst traffic run, then kill between API ACK and cursor commit.
        tokio::time::sleep(Duration::from_secs(burst_start + burst_duration + 1)).await;
        std::fs::write(root.join("pause"), b"inject").unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if rows(&root, "consumer")
                .iter()
                .any(|v| v["kind"] == "ack_before_cursor")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "No injected ACK boundary reached"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        drop(consumer);
        drop(server);
        std::fs::remove_file(root.join("pause")).unwrap();
        stopped_at = started.elapsed().as_secs_f64();
        println!(
            "Forced consumer/API stop at {stopped_at:.2}s after durable ACK, before cursor commit"
        );
        tokio::time::sleep(Duration::from_secs(3)).await; // producers continue, Kafka accumulates backlog.
        let restart = Instant::now();
        server = api(&root, port, count as usize);
        wait_ready(&http, &url).await;
        recovery_s = restart.elapsed().as_secs_f64();
        println!("GPU API recovered in {recovery_s:.2}s; resuming retained record");
        consumer = stream(&root, &consume, "consumer", false);
        let resuming = Instant::now();
        wait_consumer(&root, &mut consumer, 2).await;
        consumer_recovery_s = resuming.elapsed().as_secs_f64();
    }
    let mut producer_summary = Vec::new();
    for p in producers {
        producer_summary.push(
            tokio::time::timeout(Duration::from_secs(seconds + 180), p)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    let produced_at = producer_summary
        .iter()
        .map(|v| v["elapsed_s"].as_f64().unwrap())
        .fold(0.0, f64::max);
    let producer_records: u64 = producer_summary
        .iter()
        .map(|v| v["records"].as_u64().unwrap())
        .sum();
    let producer_events: u64 = producer_summary
        .iter()
        .map(|v| v["events"].as_u64().unwrap())
        .sum();
    let kafka = rskafka::client::ClientBuilder::new(vec![broker.clone()])
        .build()
        .await
        .unwrap();
    let mut expected_records = 0u64;
    for source in ["internal", "provider"] {
        let p = kafka
            .partition_client(
                format!("{run}-{source}"),
                0,
                rskafka::client::partition::UnknownTopicHandling::Error,
            )
            .await
            .unwrap();
        expected_records += p
            .get_offset(rskafka::client::partition::OffsetAt::Latest)
            .await
            .unwrap() as u64;
    }
    assert!(expected_records >= producer_records);
    let drain = Instant::now();
    loop {
        assert!(
            consumer.0.try_wait().unwrap().is_none(),
            "Consumer failed; pending preserved, inspect evidence"
        );
        let db = rusqlite::Connection::open(root.join("cursor.sqlite3")).unwrap();
        let cursor: i64 = db
            .query_row(
                "SELECT coalesce(sum(next_offset),0) FROM cursors",
                [],
                |r| r.get(0),
            )
            .unwrap();
        drop(db);
        if cursor as u64 == expected_records {
            break;
        }
        assert!(drain.elapsed() < Duration::from_secs(180), "Drain timeout");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let all_durable_s = started.elapsed().as_secs_f64();
    drop(consumer);
    let cursor_db = rusqlite::Connection::open(root.join("cursor.sqlite3")).unwrap();
    let committed: Vec<Value> = cursor_db
        .prepare("SELECT metrics FROM completions ORDER BY topic,offset")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| serde_json::from_str(&r.unwrap()).unwrap())
        .collect();
    let expected_events: u64 = committed
        .iter()
        .map(|v| v["events"].as_u64().unwrap())
        .sum();
    assert!(expected_events >= producer_events);
    if mode == "combined" || mode == "restart" {
        assert!(
            committed.iter().any(|v| v["duplicate"] == true),
            "Lost ACK was not retried"
        );
    }
    assert_eq!(committed.len() as u64, expected_records);
    assert_eq!(
        committed
            .iter()
            .map(|v| v["events"].as_u64().unwrap())
            .sum::<u64>(),
        expected_events
    );
    // A second forced restart after drain validates final persisted state.
    drop(server);
    server = api(&root, port, count as usize);
    wait_ready(&http, &url).await;
    let mut checks = 0;
    for i in (0..count.min(2000))
        .map(|n| (n * 104729) % count)
        .chain(std::iter::once(count - 1))
    {
        let response = http
            .get(format!("{url}/v1/gpu/payments/{run}-{i}"))
            .bearer_auth(KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let v: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(v["internal"]["amount_minor"], 10000 + i);
        assert_eq!(
            v["provider"]["amount_minor"],
            10000 + i - u64::from(i.is_multiple_of(10))
        );
        assert_eq!(v["internal"]["version"], 1);
        assert_eq!(v["provider"]["version"], 2);
        let expected = if i.is_multiple_of(20) {
            "AmountAndStatusMismatch"
        } else if i.is_multiple_of(10) {
            "AmountMismatch"
        } else {
            "Matched"
        };
        assert_eq!(v["status"], expected);
        checks += 1;
    }
    let db = rusqlite::Connection::open(root.join("gpu.sqlite3")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM gpu_event_ids", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        (count * 3) as i64
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM gpu_requests", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        committed
            .iter()
            .map(|v| v["request_id"].as_str().unwrap())
            .collect::<std::collections::HashSet<_>>()
            .len() as i64
    );
    assert_eq!(
        db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    let latency: Vec<f64> = committed
        .iter()
        .map(|v| v["end_to_end_ms"].as_f64().unwrap())
        .collect();
    let startup_profiles: Vec<Value> = std::fs::read_to_string(root.join("api.err.log"))
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("GPU_PROFILE "))
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .filter(|v| v["phase"] == "startup")
        .collect();
    let mut timeline = std::collections::BTreeMap::<i64, Vec<&Value>>::new();
    for row in &committed {
        timeline
            .entry((row["committed_at_ms"].as_i64().unwrap() - started_ms) / 1000)
            .or_default()
            .push(row);
    }
    let timeline:Vec<Value>=timeline.into_iter().map(|(second,rows)|json!({"second":second,"events":rows.iter().map(|r|r["events"].as_u64().unwrap()).sum::<u64>(),"records":rows.len(),"record_latency_p95_ms":pct(rows.iter().map(|r|r["end_to_end_ms"].as_f64().unwrap()).collect(),0.95),"max_fetch_lag_records":rows.iter().map(|r|r["remaining_records_at_fetch"].as_u64().unwrap()).max().unwrap()})).collect();
    std::fs::write(
        root.join("completions.json"),
        serde_json::to_vec(&committed).unwrap(),
    )
    .unwrap();
    let profiles: Vec<Value> = std::fs::read_to_string(root.join("api.err.log"))
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("GPU_PROFILE "))
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .filter(|v| v["phase"] == "apply")
        .collect();
    let mut stages = serde_json::Map::new();
    for field in [
        "queue_ms",
        "preparation_ms",
        "identity_ms",
        "compute_ms",
        "journal_ms",
        "commit_ms",
        "total_ms",
    ] {
        let values: Vec<f64> = profiles.iter().filter_map(|v| v[field].as_f64()).collect();
        if !values.is_empty() {
            stages.insert(field.into(),json!({"p50":pct(values.clone(),0.5),"p95":pct(values.clone(),0.95),"p99":pct(values,0.99)}));
        }
    }
    let report = json!({"validated":true,"gpu_stages_ms":stages,"consumer_concurrency":std::env::var("PAYMENT_STREAM_CONCURRENCY").unwrap_or("2".into()),"batch_events":std::env::var("PAYMENT_STREAM_BATCH_EVENTS").unwrap_or("2048".into()),"startup_profiles":startup_profiles,"timeline":timeline,"producer_acknowledged_records":producer_records,"payments":count,"unique_source_events":count*3,"delivered_events_including_duplicates":expected_events,"kafka_records":expected_records,"started_at_ms":started_ms,"configured_seconds":seconds,"base_payments_per_source_per_s":rate,"burst_payments_per_source_per_s":burst_rate,"burst_start_s":burst_start,"burst_duration_s":burst_duration,"producers_finished_s":produced_at,"all_durable_s":all_durable_s,"effective_unique_events_per_s":count as f64*3.0/all_durable_s,"stop_at_s":stopped_at,"restart_to_ready_s":recovery_s,"consumer_restart_to_ready_s":consumer_recovery_s,"initial_consumer_ready_before_generation":true,"post_producer_drain_s":all_durable_s-produced_at,"snapshot_checks":checks,"end_to_end_record_latency_ms":{"p50":pct(latency.clone(),0.5),"p95":pct(latency.clone(),0.95),"p99":pct(latency.clone(),0.99),"max":pct(latency,1.0)},"producer_summaries":producer_summary,"mode":mode,"ambiguous_retry_confirmed":committed.iter().any(|v|v["duplicate"]==true),"broker":"Apache Kafka 4.0.1, one local broker, replication factor 1","durability":"GPU journal and consumer cursor SQLite WAL synchronous FULL; pending retained before HTTP; cursor advances after durable ACK"});
    std::fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    drop(server);
}
