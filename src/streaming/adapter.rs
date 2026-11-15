//! Explicit JSON-pointer mapping. Amounts are integer minor units, never floats.
use super::*;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub event_id: String,
    pub payment_id: String,
    pub version: String,
    pub amount_minor: String,
    pub currency: String,
    pub status: String,
    pub statuses: BTreeMap<String, String>,
}
impl Mapping {
    pub fn normalize(&self, row: &Value, source: &str) -> Result<Value> {
        if !["Internal", "Provider"].contains(&source) {
            return Err("Invalid source".into());
        }
        let field = |pointer: &str| -> Result<&Value> {
            if !pointer.starts_with('/') {
                return Err("Mapping fields must be JSON pointers".into());
            }
            row.pointer(pointer)
                .ok_or_else(|| format!("Missing mapped field {pointer}").into())
        };
        let text = |pointer: &str, max: usize| -> Result<String> {
            let s = field(pointer)?.as_str().ok_or("Expected mapped string")?;
            if s.trim().is_empty() || s.len() > max {
                return Err("Invalid mapped string length".into());
            }
            Ok(s.into())
        };
        let event_id = text(&self.event_id, 128)?;
        let id = text(&self.payment_id, 256)?;
        let version = field(&self.version)?
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or("Positive integer source version required")?;
        let amount = field(&self.amount_minor)?
            .as_u64()
            .ok_or("Amount must be a nonnegative integer in minor units")?;
        let currency = text(&self.currency, 3)?.to_uppercase();
        let raw = text(&self.status, 128)?;
        let status = self.statuses.get(&raw).ok_or("Unmapped source status")?;
        // Reuse public payment validation, including supported currencies and statuses.
        let payment: crate::Payment = serde_json::from_value(
            json!({"id":id,"currency":currency,"amount_minor":amount,"status":status}),
        )?;
        Ok(json!({"event_id":event_id,"source":source,"version":version,"payment":payment}))
    }
}
pub(super) async fn import(
    client: &rskafka::client::Client,
    run: &str,
    args: &[String],
) -> Result<()> {
    let source = arg(args, 4)?;
    if !["Internal", "Provider"].contains(&source) {
        return Err("Invalid source".into());
    }
    let mapping: Mapping = serde_json::from_slice(&std::fs::read(arg(args, 5)?)?)?;
    let path = arg(args, 6)?;
    if std::fs::metadata(path)?.len() > 16_777_216 {
        return Err("Import limited to 16 MiB".into());
    }
    let rows: Vec<Value> = serde_json::from_slice(&std::fs::read(path)?)?;
    if rows.is_empty() || rows.len() > 10000 {
        return Err("Import requires 1..10000 records".into());
    }
    let events: Vec<Value> = rows
        .iter()
        .map(|r| mapping.normalize(r, source))
        .collect::<Result<_>>()?;
    let mut identities = std::collections::HashMap::new();
    for event in &events {
        let id = event["event_id"].as_str().unwrap();
        if identities.insert(id, event).is_some_and(|old| old != event) {
            return Err("An event ID has conflicting imported content".into());
        }
    }
    let partition = client
        .partition_client(
            format!("{run}-{}", source.to_lowercase()),
            0,
            UnknownTopicHandling::Error,
        )
        .await?;
    // Flush before a repeated payment so every source version remains observable.
    let mut batch = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for event in events {
        let id = event["payment"]["id"].as_str().unwrap().to_string();
        if batch.len() == 512 || ids.contains(&id) {
            publish(&partition, std::mem::take(&mut batch)).await?;
            ids.clear();
        }
        ids.insert(id);
        batch.push(event);
    }
    if !batch.is_empty() {
        publish(&partition, batch).await?;
    }
    Ok(())
}
async fn publish(
    partition: &rskafka::client::partition::PartitionClient,
    events: Vec<Value>,
) -> Result<()> {
    let data = serde_json::to_vec(&Envelope {
        schema: 1,
        created_at_ms: Utc::now().timestamp_millis(),
        events,
    })?;
    tokio::time::timeout(
        Duration::from_secs(30),
        partition.produce(
            vec![Record {
                key: None,
                value: Some(data),
                headers: BTreeMap::new(),
                timestamp: Utc::now(),
            }],
            Compression::NoCompression,
        ),
    )
    .await??;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maps_explicit_versions_and_rejects_lossy_money_and_unknown_status() {
        let m:Mapping=serde_json::from_value(json!({"event_id":"/event","payment_id":"/payment","version":"/revision","amount_minor":"/amount","currency":"/currency","status":"/state","statuses":{"paid":"Completed"}})).unwrap();
        let mut row = json!({"event":"e1","payment":"p1","revision":2,"amount":100,"currency":"gbp","state":"paid"});
        assert_eq!(
            m.normalize(&row, "Provider").unwrap()["payment"]["currency"],
            "GBP"
        );
        row["amount"] = json!(1.2);
        assert!(m.normalize(&row, "Provider").is_err());
        row["amount"] = json!(100);
        row["state"] = json!("unknown");
        assert!(m.normalize(&row, "Provider").is_err());
    }
}
