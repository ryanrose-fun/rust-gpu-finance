use super::*;
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

pub(super) async fn status(client: &rskafka::client::Client, run: &str, path: &str) -> Result<()> {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_secs(2))?;
    let mut sources = Vec::new();
    for source in ["internal", "provider"] {
        let topic = format!("{run}-{source}");
        let partition = client
            .partition_client(topic.clone(), 0, UnknownTopicHandling::Error)
            .await?;
        let high = tokio::time::timeout(
            Duration::from_secs(30),
            partition.get_offset(OffsetAt::Latest),
        )
        .await??;
        let offset: i64 = db.query_row(
            "SELECT coalesce((SELECT next_offset FROM cursors WHERE topic=?1),0)",
            [&topic],
            |r| r.get(0),
        )?;
        let age: Option<i64> = db.query_row(
            "SELECT min(json_extract(payload,'$.created_ms')) FROM pending_batches WHERE topic=?1",
            [&topic],
            |r| r.get(0),
        )?;
        sources.push(json!({"topic":topic,"durable_offset":offset,"high_watermark":high,"lag_records":(high-offset).max(0),"oldest_pending_age_ms":age.map(|t|(Utc::now().timestamp_millis()-t).max(0))}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"sources":sources,"observed_at_ms":Utc::now().timestamp_millis()})
        )?
    );
    Ok(())
}

fn setting(name: &str, default: usize, max: usize) -> Result<usize> {
    let value = std::env::var(name)
        .unwrap_or(default.to_string())
        .parse::<usize>()?;
    if value == 0 || value > max {
        return Err(format!("{name} must be 1..{max}").into());
    }
    Ok(value)
}

pub(super) async fn consume(
    client: &rskafka::client::Client,
    broker: &str,
    run: &str,
    api: &str,
    key: &str,
    path: &str,
) -> Result<()> {
    let concurrency = setting("PAYMENT_STREAM_CONCURRENCY", 2, 2)?;
    let max_events = setting("PAYMENT_STREAM_BATCH_EVENTS", 2048, 4096)?;
    let max_bytes = setting("PAYMENT_STREAM_BATCH_BYTES", 1_048_576, 8_388_608)?;
    let store = Arc::new(Mutex::new(store::Store::open(
        path,
        &format!("{broker}|{run}|{api}"),
    )?));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let slots = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = tokio::task::JoinSet::new();
    for source in ["internal", "provider"] {
        let topic = format!("{run}-{source}");
        let partition = client
            .partition_client(topic.clone(), 0, UnknownTopicHandling::Error)
            .await?;
        let source_key = std::env::var(format!("PAYMENT_STREAM_{}_KEY", source.to_uppercase()))
            .unwrap_or(key.to_string());
        if source_key.is_empty() {
            return Err("Source credential is required".into());
        }
        let (store, http, slots, api, key) = (
            store.clone(),
            http.clone(),
            slots.clone(),
            api.to_string(),
            source_key,
        );
        // One ordered owner per partition. Different sources can progress concurrently.
        tasks.spawn(async move {
            let mut buffered = VecDeque::new();
            let mut high = 0;
            loop {
                let pending = store.lock().map_err(|_| "Cursor lock poisoned")?.pending(&topic)?;
                if let Some(pending) = pending {
                    let _slot = slots.acquire().await?;
                    deliver(&http,&api,&key,&store,pending).await?;
                    continue;
                }
                let offset = store.lock().map_err(|_| "Cursor lock poisoned")?.offset(&topic)?;
                let mut fetch_ms = 0.;
                if buffered.is_empty() {
                    let fetch = Instant::now();
                    let (records, watermark) = tokio::time::timeout(Duration::from_secs(30),partition.fetch_records(offset,1..4_194_304,5)).await??;
                    high = watermark;
                    if high < offset { return Err("Kafka truncated below durable cursor".into()); }
                    for r in records {
                        buffered.push_back((r.offset, r.record.value.ok_or("Tombstone in payment log")?));
                    }
                    fetch_ms = fetch.elapsed().as_secs_f64()*1000.;
                    if buffered.is_empty() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                }
                let preparing = Instant::now();
                let pending = batch(&topic, offset, high, buffered.iter().cloned().map(Ok), max_events,max_bytes)?;
                let preparation_ms = preparing.elapsed().as_secs_f64()*1000.;
                let preparing = Instant::now();
                store.lock().map_err(|_| "Cursor lock poisoned")?.prepare(&pending)?;
                // Only discard records after their exact request is durable.
                buffered.drain(..pending.records.len());
                println!("{}",json!({"kind":"prepared","topic":topic,"offset":offset,"records":pending.records.len(),"events":pending.events,"fetch_ms":fetch_ms,"preparation_ms":preparation_ms,"pending_commit_ms":preparing.elapsed().as_secs_f64()*1000.,"remaining_records":high-offset,"oldest_pending_age_ms":(Utc::now().timestamp_millis()-pending.created_ms).max(0)}));
                let waiting = Instant::now();
                let _slot = slots.acquire().await?;
                println!("{}",json!({"kind":"admitted","topic":topic,"offset":offset,"admission_ms":waiting.elapsed().as_secs_f64()*1000.}));
                deliver(&http,&api,&key,&store,pending).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(),Box<dyn Error+Send+Sync>>(())
        });
    }
    // Any failed owner stops the whole consumer. Other pending work stays durable.
    println!(
        "{}",
        json!({"kind":"consumer_ready","concurrency":concurrency,"batch_events":max_events,"batch_bytes":max_bytes})
    );
    let result = tasks.join_next().await.ok_or("No partition workers")?;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result?
}

fn batch(
    topic: &str,
    offset: i64,
    high: i64,
    records: impl Iterator<Item = Result<(i64, Vec<u8>)>>,
    max_events: usize,
    max_bytes: usize,
) -> Result<store::Pending> {
    let source = if topic.ends_with("-internal") {
        "Internal"
    } else {
        "Provider"
    };
    let mut events = Vec::new();
    let mut info = Vec::new();
    let mut pairs = HashSet::new();
    let mut ids = HashSet::new();
    let mut bytes = 256usize; // request envelope allowance
    for record in records {
        let (next, data) = record?;
        if next != offset + info.len() as i64 {
            return Err("Kafka retention gap; refusing to skip evidence".into());
        }
        if data.len() > 1_048_576 {
            return Err("Record exceeds 1 MiB".into());
        }
        let env: Envelope = serde_json::from_slice(&data)?;
        if env.schema != 1
            || env.events.is_empty()
            || env.events.len() > 512
            || env.events.iter().any(|e| e["source"] != source)
        {
            return Err("Invalid source envelope".into());
        }
        let mut next_ids = HashSet::new();
        let mut next_pairs = HashSet::new();
        for e in &env.events {
            let id = e["event_id"]
                .as_str()
                .ok_or("Missing event ID")?
                .to_string();
            let payment = e["payment"]["id"]
                .as_str()
                .ok_or("Missing payment ID")?
                .to_string();
            if !next_ids.insert(id) || !next_pairs.insert(payment) {
                return Err("Repeated ID inside Kafka record".into());
            }
        }
        // Never merge successive versions of one payment; the GPU preserves each transition.
        let fits = events.len() + env.events.len() <= max_events
            && bytes + data.len() <= max_bytes
            && ids.is_disjoint(&next_ids)
            && pairs.is_disjoint(&next_pairs);
        if !fits {
            if info.is_empty() {
                return Err("Record exceeds configured batch limits".into());
            }
            break;
        }
        bytes += data.len();
        ids.extend(next_ids);
        pairs.extend(next_pairs);
        info.push(store::RecordInfo {
            offset: next,
            created_ms: env.created_at_ms,
            events: env.events.len(),
        });
        events.extend(env.events);
    }
    let first = info.first().ok_or("Empty batch")?;
    let id = if info.len() == 1 {
        request_id(topic, offset)
    } else {
        format!(
            "kafka-{:x}",
            Sha256::digest(format!(
                "{topic}:0:{offset}:{}",
                info.last().unwrap().offset
            ))
        )
    };
    Ok(store::Pending {
        topic: topic.into(),
        offset,
        created_ms: first.created_ms,
        events: events.len(),
        high_watermark: high,
        body: serde_json::to_string(&json!({"request_id":id,"events":events}))?,
        records: info,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(offset: i64, id: u64) -> Result<(i64, Vec<u8>)> {
        Ok((
            offset,
            serde_json::to_vec(&Envelope {
                schema: 1,
                created_at_ms: 1,
                events: vec![event("test", id, "Internal", true)],
            })
            .unwrap(),
        ))
    }
    #[test]
    fn fetched_records_survive_batch_boundaries() {
        let mut buffered: VecDeque<_> = (0..5).map(|i| record(i, i as u64).unwrap()).collect();
        let mut offsets = Vec::new();
        while !buffered.is_empty() {
            let pending = batch(
                "test-internal",
                offsets.len() as i64,
                5,
                buffered.iter().cloned().map(Ok),
                2,
                1_048_576,
            )
            .unwrap();
            offsets.extend(pending.records.iter().map(|r| r.offset));
            buffered.drain(..pending.records.len());
        }
        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    }
    #[test]
    fn batches_distinct_payments_but_preserves_repeated_versions() {
        let p = batch(
            "test-internal",
            0,
            3,
            vec![record(0, 1), record(1, 2), record(2, 1)].into_iter(),
            4096,
            1_048_576,
        )
        .unwrap();
        assert_eq!(p.records.len(), 2);
        assert_eq!(p.events, 2);
        let p = batch(
            "test-internal",
            0,
            2,
            vec![record(0, 1), record(1, 2)].into_iter(),
            1,
            1_048_576,
        )
        .unwrap();
        assert_eq!(p.records.len(), 1);
        assert!(
            batch(
                "test-internal",
                0,
                2,
                vec![record(1, 1)].into_iter(),
                4096,
                1_048_576
            )
            .is_err()
        );
    }
}
