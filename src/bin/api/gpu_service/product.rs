//! Durable product views derived from the authoritative GPU journal, never CPU reconciliation.
use super::*;
use serde_json::{Value, json};

#[derive(Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Note {
    pub request_id: String,
    pub note: String,
    pub acknowledged: Option<bool>,
}
pub enum Query {
    FirstSeen {
        id: String,
    },
    Feed {
        after: i64,
        limit: usize,
    },
    History {
        id: String,
        after: i64,
        limit: usize,
    },
    Notes {
        id: String,
        after: i64,
        limit: usize,
    },
    Annotate {
        id: String,
        actor: String,
        note: Note,
    },
}

pub fn initialize(db: &mut Connection, tenant: &str) -> Result<(), String> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS gpu_product_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1),tenant TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS gpu_payment_requests(payment_id TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(payment_id,seq)) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS gpu_request_times(seq INTEGER PRIMARY KEY,committed_at_ms INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS gpu_product_progress(singleton INTEGER PRIMARY KEY CHECK(singleton=1),seq INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS gpu_notes(seq INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL UNIQUE,payment_id TEXT NOT NULL,actor TEXT NOT NULL,payload TEXT NOT NULL,created_at_ms INTEGER NOT NULL);
      CREATE INDEX IF NOT EXISTS gpu_notes_payment ON gpu_notes(payment_id,seq);") .map_err(|e|e.to_string())?;
    db.execute(
        "INSERT OR IGNORE INTO gpu_product_owner VALUES(1,?1)",
        [tenant],
    )
    .map_err(|e| e.to_string())?;
    let owner: String = db
        .query_row("SELECT tenant FROM gpu_product_owner", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if owner != tenant {
        return Err("GPU journal belongs to another tenant; refusing reuse".into());
    }
    // Backfill only the query index of historical requests. Original evidence is untouched.
    let after: i64 = db
        .query_row(
            "SELECT coalesce((SELECT seq FROM gpu_product_progress),0)",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let tx = db.transaction().map_err(|e| e.to_string())?;
    tx.execute("INSERT OR IGNORE INTO gpu_payment_requests SELECT json_extract(e.value,'$.payment.id'),r.seq FROM gpu_requests r,json_each(r.input,'$.events') e WHERE r.seq>?1",[after]).map_err(|e|e.to_string())?;
    tx.execute("INSERT INTO gpu_product_progress VALUES(1,(SELECT coalesce(max(seq),0) FROM gpu_requests)) ON CONFLICT(singleton) DO UPDATE SET seq=excluded.seq",[]).map_err(|e|e.to_string())?;
    tx.commit().map_err(|e| e.to_string())
}

pub fn index(tx: &rusqlite::Transaction<'_>, seq: i64, request: &Submit) -> rusqlite::Result<()> {
    let mut statement =
        tx.prepare_cached("INSERT OR IGNORE INTO gpu_payment_requests VALUES(?1,?2)")?;
    for event in &request.events {
        statement.execute(params![event.payment.id, seq])?;
    }
    tx.execute(
        "INSERT INTO gpu_request_times VALUES(?1,?2)",
        params![seq, chrono::Utc::now().timestamp_millis()],
    )?;
    tx.execute("INSERT INTO gpu_product_progress VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET seq=excluded.seq",[seq])?;
    Ok(())
}

pub fn query(db: &mut Connection, query: Query) -> Answer {
    execute(db, query).map_err(|e| {
        eprintln!("GPU product query failed: {e}");
        Error::Unavailable
    })?
}
fn execute(db: &mut Connection, query: Query) -> Result<Answer, Box<dyn std::error::Error>> {
    match query {
        Query::FirstSeen { id } => {
            let first:Option<i64>=db.query_row("SELECT (SELECT t.committed_at_ms FROM gpu_payment_requests p LEFT JOIN gpu_request_times t ON t.seq=p.seq WHERE p.payment_id=?1 ORDER BY p.seq LIMIT 1)",[id],|r|r.get(0))?;
            Ok(Ok(json!({"first_seen_at_ms":first})))
        }
        Query::Feed { after, limit }
        | Query::History {
            after,
            limit,
            id: _,
        } if after < 0 || !(1..=20).contains(&limit) => Ok(Err(Error::Invalid(
            "after >= 0 and limit 1..20 required".into(),
        ))),
        Query::Feed { after, limit } => page(db, after, limit, None),
        Query::History { id, after, limit } => page(db, after, limit, Some(&id)),
        Query::Notes { id, after, limit } => {
            if after < 0 || !(1..=100).contains(&limit) {
                return Ok(Err(Error::Invalid("Invalid note cursor/limit".into())));
            }
            let mut q=db.prepare("SELECT seq,actor,payload,created_at_ms FROM gpu_notes WHERE payment_id=?1 AND seq>?2 ORDER BY seq LIMIT ?3")?;
            let rows = q.query_map(params![id, after, limit as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?;
            let mut items = Vec::new();
            let mut next = after;
            for row in rows {
                let (seq, actor, payload, time) = row?;
                next = seq;
                items.push(json!({"sequence":seq,"actor":actor,"annotation":serde_json::from_str::<Value>(&payload)?,"created_at_ms":time}));
            }
            Ok(Ok(json!({"entries":items,"next_cursor":next})))
        }
        Query::Annotate { id, actor, note } => {
            if note.request_id.trim().is_empty()
                || note.request_id.len() > 128
                || note.note.len() > 4000
                || (note.note.trim().is_empty() && note.acknowledged.is_none())
            {
                return Ok(Err(Error::Invalid(
                    "A stable request ID and note (up to 4000 bytes) or acknowledgement required"
                        .into(),
                )));
            }
            let payload = canonical(&note);
            let existing: Option<(String, String, String, i64)> = db
                .query_row(
                    "SELECT payment_id,actor,payload,seq FROM gpu_notes WHERE request_id=?1",
                    [&note.request_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if let Some((old_id, old_actor, old_payload, seq)) = existing {
                return Ok(
                    if old_id == id && old_actor == actor && old_payload == payload {
                        Ok(json!({"sequence":seq,"duplicate":true}))
                    } else {
                        Err(Error::Conflict)
                    },
                );
            }
            if !db.query_row(
                "SELECT EXISTS(SELECT 1 FROM gpu_payment_requests WHERE payment_id=?1)",
                [&id],
                |r| r.get::<_, bool>(0),
            )? {
                return Ok(Err(Error::NotFound));
            }
            let tx = db.transaction()?;
            tx.execute("INSERT INTO gpu_notes(request_id,payment_id,actor,payload,created_at_ms) VALUES(?1,?2,?3,?4,?5)",params![note.request_id,id,actor,payload,chrono::Utc::now().timestamp_millis()])?;
            let seq = tx.last_insert_rowid();
            tx.commit()?;
            Ok(Ok(json!({"sequence":seq,"duplicate":false})))
        }
    }
}
fn page(
    db: &Connection,
    after: i64,
    limit: usize,
    id: Option<&str>,
) -> Result<Answer, Box<dyn std::error::Error>> {
    let sql = if id.is_some() {
        "SELECT r.seq,r.request_id,r.input,r.computed,t.committed_at_ms FROM gpu_payment_requests p JOIN gpu_requests r ON r.seq=p.seq LEFT JOIN gpu_request_times t ON t.seq=r.seq WHERE p.payment_id=?3 AND p.seq>?1 ORDER BY p.seq LIMIT ?2"
    } else {
        "SELECT r.seq,r.request_id,r.input,r.computed,t.committed_at_ms FROM gpu_requests r LEFT JOIN gpu_request_times t ON t.seq=r.seq WHERE r.seq>?1 AND (?3 IS NULL) ORDER BY r.seq LIMIT ?2"
    };
    let mut q = db.prepare(sql)?;
    let rows = q.query_map(params![after, limit as i64, id], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<i64>>(4)?,
        ))
    })?;
    let mut items = Vec::new();
    let mut next = after;
    for row in rows {
        let (seq, request, input, computed, time) = row?;
        next = seq;
        let input: Submit = serde_json::from_str(&input)?;
        let computed: Computed = serde_json::from_str(&computed)?;
        let events: Vec<_> = input
            .events
            .into_iter()
            .filter(|e| id.is_none_or(|id| e.payment.id == id))
            .collect();
        let changes: Vec<_> = computed
            .changes
            .into_iter()
            .filter(|c| id.is_none_or(|id| c.id == id))
            .collect();
        let receipts: Vec<_> = computed
            .receipts
            .into_iter()
            .filter(|r| id.is_none_or(|id| r.id == id))
            .collect();
        items.push(json!({"sequence":seq,"request_id":request,"committed_at_ms":time,"changes":changes,"receipts":receipts,"evidence":events}));
    }
    Ok(Ok(
        json!({"entries":items,"next_cursor":next,"cursor_semantics":"exclusive durable request sequence; persist only after processing"}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn notes_are_idempotent_and_separate_from_evidence_and_owner_is_bound() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE gpu_requests(seq INTEGER PRIMARY KEY,request_id TEXT,input TEXT,computed TEXT);").unwrap();
        initialize(&mut db, "bank-a").unwrap();
        assert!(initialize(&mut db, "bank-b").is_err());
        db.execute("INSERT INTO gpu_payment_requests VALUES('p',1)", [])
            .unwrap();
        let note = Note {
            request_id: "n".into(),
            note: "Checked provider".into(),
            acknowledged: Some(true),
        };
        let make = || Query::Annotate {
            id: "p".into(),
            actor: "auditor-1".into(),
            note: note.clone(),
        };
        assert_eq!(query(&mut db, make()).unwrap()["duplicate"], false);
        assert_eq!(query(&mut db, make()).unwrap()["duplicate"], true);
        assert_eq!(
            db.query_row("SELECT count(*) FROM gpu_requests", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let value = query(
            &mut db,
            Query::Notes {
                id: "p".into(),
                after: 0,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(value["entries"].as_array().unwrap().len(), 1);
    }
}
