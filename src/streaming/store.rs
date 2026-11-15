use super::Result;
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct RecordInfo {
    pub offset: i64,
    pub created_ms: i64,
    pub events: usize,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Pending {
    pub topic: String,
    pub offset: i64,
    pub body: String,
    pub created_ms: i64,
    pub events: usize,
    pub high_watermark: i64,
    #[serde(default)]
    pub records: Vec<RecordInfo>,
}
pub struct Store {
    db: Connection,
    _lock: File,
}
impl Store {
    pub fn open(path: impl AsRef<Path>, binding: &str) -> Result<Self> {
        let path = path.as_ref();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.try_lock_exclusive()?;
        let db = Connection::open(path)?;
        let tables: i64 = db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )?;
        let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        if tables > 0 && app != 0x4b475031 {
            return Err("Not a Kafka GPU cursor database".into());
        }
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.pragma_update(None, "application_id", 0x4b475031)?;
        let integrity: String = db.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        if integrity != "ok" {
            return Err("Cursor database integrity failure".into());
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS config(singleton INTEGER PRIMARY KEY CHECK(singleton=1),binding TEXT NOT NULL); CREATE TABLE IF NOT EXISTS cursors(topic TEXT PRIMARY KEY,next_offset INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS completions(topic TEXT NOT NULL,offset INTEGER NOT NULL,metrics TEXT NOT NULL,PRIMARY KEY(topic,offset)); CREATE TABLE IF NOT EXISTS pending(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload TEXT NOT NULL);")?;
        db.execute("INSERT OR IGNORE INTO config VALUES(1,?1)", [binding])?;
        let old: String = db.query_row("SELECT binding FROM config", [], |r| r.get(0))?;
        if old != binding {
            return Err("Cursor database belongs to another broker/run/API".into());
        }
        // Upgrade the original singleton pending request without changing its exact bytes.
        db.execute_batch("BEGIN IMMEDIATE; CREATE TABLE IF NOT EXISTS pending_batches(topic TEXT PRIMARY KEY,payload TEXT NOT NULL); INSERT OR IGNORE INTO pending_batches SELECT json_extract(payload,'$.topic'),payload FROM pending; DELETE FROM pending; COMMIT;")?;
        Ok(Self { db, _lock: lock })
    }
    pub fn offset(&self, topic: &str) -> Result<i64> {
        Ok(self
            .db
            .query_row(
                "SELECT next_offset FROM cursors WHERE topic=?1",
                [topic],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }
    pub fn pending(&self, topic: &str) -> Result<Option<Pending>> {
        let raw: Option<String> = self
            .db
            .query_row(
                "SELECT payload FROM pending_batches WHERE topic=?1",
                [topic],
                |r| r.get(0),
            )
            .optional()?;
        Ok(raw.map(|s| serde_json::from_str(&s)).transpose()?)
    }
    pub fn prepare(&mut self, p: &Pending) -> Result<()> {
        if self.offset(&p.topic)? != p.offset {
            return Err("Non-contiguous prepared cursor".into());
        }
        self.db.execute(
            "INSERT INTO pending_batches VALUES(?1,?2)",
            params![p.topic, serde_json::to_string(p)?],
        )?;
        Ok(())
    }
    pub fn complete(&mut self, p: &Pending, metrics: &serde_json::Value) -> Result<()> {
        let tx = self.db.transaction()?;
        let raw: String = tx.query_row(
            "SELECT payload FROM pending_batches WHERE topic=?1",
            [&p.topic],
            |r| r.get(0),
        )?;
        if serde_json::to_string(&serde_json::from_str::<Pending>(&raw)?)?
            != serde_json::to_string(p)?
        {
            return Err("Pending record changed".into());
        }
        let fallback = [RecordInfo {
            offset: p.offset,
            created_ms: p.created_ms,
            events: p.events,
        }];
        let records = if p.records.is_empty() {
            &fallback[..]
        } else {
            &p.records
        };
        if records
            .iter()
            .enumerate()
            .any(|(i, r)| r.offset != p.offset + i as i64)
        {
            return Err("Non-contiguous pending batch".into());
        }
        tx.execute("INSERT INTO cursors VALUES(?1,?2) ON CONFLICT(topic) DO UPDATE SET next_offset=excluded.next_offset",params![p.topic,p.offset+records.len() as i64])?;
        for record in records {
            let mut metric = metrics.clone();
            metric["offset"] = record.offset.into();
            metric["events"] = record.events.into();
            metric["created_at_ms"] = record.created_ms.into();
            metric["end_to_end_ms"] = (metric["committed_at_ms"]
                .as_i64()
                .unwrap_or(record.created_ms)
                - record.created_ms)
                .max(0)
                .into();
            tx.execute(
                "INSERT INTO completions VALUES(?1,?2,?3)",
                params![p.topic, record.offset, serde_json::to_string(&metric)?],
            )?;
        }
        tx.execute("DELETE FROM pending_batches WHERE topic=?1", [&p.topic])?;
        tx.commit()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_pending_batches_recover_and_partial_completion_rolls_back() {
        let dir = std::env::temp_dir().join(format!("parallel-cursor-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("state.db");
        let make = |topic: &str| Pending {
            topic: topic.into(),
            offset: 0,
            body: format!("exact-{topic}"),
            created_ms: 1,
            events: 2,
            high_watermark: 2,
            records: vec![
                RecordInfo {
                    offset: 0,
                    created_ms: 1,
                    events: 1,
                },
                RecordInfo {
                    offset: 1,
                    created_ms: 2,
                    events: 1,
                },
            ],
        };
        {
            let mut s = Store::open(&path, "b").unwrap();
            s.prepare(&make("a")).unwrap();
            s.prepare(&make("b")).unwrap();
        }
        {
            let mut s = Store::open(&path, "b").unwrap();
            assert!(s.pending("a").unwrap().is_some());
            assert!(s.pending("b").unwrap().is_some());
            s.db.execute_batch("CREATE TRIGGER fail_second BEFORE INSERT ON completions WHEN NEW.offset=1 BEGIN SELECT RAISE(ABORT,'failure after first record'); END;").unwrap();
            assert!(
                s.complete(&make("b"), &serde_json::json!({"committed_at_ms":3}))
                    .is_err()
            );
            assert_eq!(s.offset("b").unwrap(), 0);
            assert_eq!(
                s.db.query_row("SELECT count(*) FROM completions", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
            s.db.execute_batch("DROP TRIGGER fail_second").unwrap();
            s.complete(&make("b"), &serde_json::json!({"committed_at_ms":3}))
                .unwrap();
            assert_eq!(s.offset("a").unwrap(), 0);
            assert_eq!(s.offset("b").unwrap(), 2);
            assert!(s.complete(&make("b"), &serde_json::json!({})).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn original_pending_payload_migrates_without_changing_request_bytes() {
        let dir = std::env::temp_dir().join(format!("old-cursor-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("state.db");
        {
            let s = Store::open(&path, "b").unwrap();
            s.db.execute("INSERT INTO pending VALUES(1,?1)",[r#"{"topic":"a","offset":0,"body":"original request","created_ms":1,"events":1,"high_watermark":1}"#]).unwrap();
        }
        {
            let mut s = Store::open(&path, "b").unwrap();
            let p = s.pending("a").unwrap().unwrap();
            assert_eq!(p.body, "original request");
            s.complete(&p, &serde_json::json!({})).unwrap();
            assert_eq!(s.offset("a").unwrap(), 1);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn pending_survives_restart_and_cursor_only_advances_on_completion() {
        let dir = std::env::temp_dir().join(format!("kafka-cursor-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("state.db");
        let p = Pending {
            topic: "a".into(),
            offset: 0,
            body: "exact bytes".into(),
            created_ms: 1,
            events: 2,
            high_watermark: 1,
            records: Vec::new(),
        };
        {
            let mut s = Store::open(&path, "binding").unwrap();
            s.prepare(&p).unwrap();
            assert_eq!(s.offset("a").unwrap(), 0);
            assert!(Store::open(&path, "binding").is_err());
        }
        {
            let mut s = Store::open(&path, "binding").unwrap();
            assert_eq!(s.pending("a").unwrap().unwrap().body, p.body);
            s.db.execute_batch("CREATE TRIGGER fail_completion BEFORE INSERT ON completions BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;").unwrap();
            assert!(
                s.complete(&p, &serde_json::json!({"duplicate":true}))
                    .is_err()
            );
            assert_eq!(s.offset("a").unwrap(), 0);
            assert!(s.pending("a").unwrap().is_some());
            s.db.execute_batch("DROP TRIGGER fail_completion;").unwrap();
            s.complete(&p, &serde_json::json!({"duplicate":true}))
                .unwrap();
            assert_eq!(s.offset("a").unwrap(), 1);
            assert!(s.pending("a").unwrap().is_none());
            assert!(s.prepare(&p).is_err());
        }
        assert!(Store::open(&path, "different").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
