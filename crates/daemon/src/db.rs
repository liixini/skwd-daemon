use std::path::PathBuf;

use rusqlite::{Connection, params};
use tracing::{debug, info};

fn xdg_data_home() -> PathBuf {
    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        format!("{home}/.local/share")
    });
    PathBuf::from(data_dir)
}

pub fn db_path() -> PathBuf {
    xdg_data_home().join("skwd-daemon").join("daemon.sqlite")
}

fn qml_db_path() -> PathBuf {
    xdg_data_home().join("quickshell/QML/OfflineStorage/Databases/7474098b8bba85e32cfdd9bd70e90282.sqlite")
}

pub fn open() -> rusqlite::Result<Connection> {
    let path = db_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let conn = Connection::open(&path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    migrate(&conn)?;
    debug!("database open at {}", path.display());
    Ok(conn)
}

fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            tags TEXT,
            colors TEXT,
            matugen TEXT,
            favourite INTEGER DEFAULT 0,
            type TEXT,
            name TEXT,
            thumb TEXT,
            thumb_sm TEXT,
            video_file TEXT,
            we_id TEXT,
            mtime INTEGER,
            hue INTEGER DEFAULT 99,
            sat INTEGER DEFAULT 0,
            analyzed_by TEXT,
            filesize INTEGER,
            width INTEGER,
            height INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_meta_favourite ON meta(favourite);
        CREATE INDEX IF NOT EXISTS idx_meta_type ON meta(type);
        CREATE INDEX IF NOT EXISTS idx_meta_name ON meta(name);
        CREATE INDEX IF NOT EXISTS idx_meta_we_id ON meta(we_id);

        CREATE TABLE IF NOT EXISTS image_optimize(
            src TEXT PRIMARY KEY,
            dest TEXT NOT NULL,
            preset TEXT NOT NULL,
            format TEXT,
            width INTEGER,
            height INTEGER,
            orig_size INTEGER,
            new_size INTEGER,
            optimized_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS video_convert(
            src TEXT PRIMARY KEY,
            dest TEXT NOT NULL,
            preset TEXT NOT NULL,
            codec TEXT,
            width INTEGER,
            height INTEGER,
            orig_size INTEGER,
            new_size INTEGER,
            converted_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS state(
            key TEXT PRIMARY KEY,
            val TEXT
        );",
    )?;

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN tags_raw TEXT", []);

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN analysis_error TEXT", []);

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN weather TEXT", []);

    Ok(())
}

pub fn import_from_qml(conn: &Connection) -> anyhow::Result<i64> {
    let src_path = qml_db_path();
    if !src_path.exists() {
        anyhow::bail!("QML database not found at {}", src_path.display());
    }

    let already: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM state WHERE key='imported_from_qml')",
        [],
        |r| r.get(0),
    )?;
    if already {
        anyhow::bail!("already imported — use wall.reimport to force");
    }

    info!("importing from {}", src_path.display());

    conn.execute_batch(&format!("ATTACH DATABASE '{}' AS qml;", src_path.display()))?;

    conn.execute_batch(
        "INSERT OR REPLACE INTO meta SELECT * FROM qml.meta;
         INSERT OR REPLACE INTO image_optimize SELECT * FROM qml.image_optimize;
         INSERT OR REPLACE INTO video_convert SELECT * FROM qml.video_convert;
         INSERT OR REPLACE INTO state SELECT * FROM qml.state WHERE key != 'imported_from_qml';",
    )?;

    let count: i64 = conn.query_row("SELECT count(*) FROM meta", [], |r| r.get(0))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    conn.execute(
        "INSERT OR REPLACE INTO state(key, val) VALUES('imported_from_qml', ?1)",
        params![now.to_string()],
    )?;

    conn.execute_batch("DETACH DATABASE qml;")?;

    info!("imported {count} wallpapers from QML database");
    Ok(count)
}

pub fn list_wallpapers(conn: &Connection, favourite_only: bool) -> rusqlite::Result<Vec<serde_json::Value>> {
    let sql = if favourite_only {
        "SELECT key, name, type, thumb, thumb_sm, favourite, hue, sat, tags, colors, matugen, video_file, we_id, analyzed_by, filesize, width, height, mtime, weather FROM meta WHERE favourite = 1 ORDER BY name"
    } else {
        "SELECT key, name, type, thumb, thumb_sm, favourite, hue, sat, tags, colors, matugen, video_file, we_id, analyzed_by, filesize, width, height, mtime, weather FROM meta ORDER BY name"
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(serde_json::json!({
            "key": row.get::<_, Option<String>>(0)?,
            "name": row.get::<_, Option<String>>(1)?,
            "type": row.get::<_, Option<String>>(2)?,
            "thumb": row.get::<_, Option<String>>(3)?,
            "thumb_sm": row.get::<_, Option<String>>(4)?,
            "favourite": row.get::<_, Option<i64>>(5)?,
            "hue": row.get::<_, Option<i64>>(6)?,
            "sat": row.get::<_, Option<i64>>(7)?,
            "tags": row.get::<_, Option<String>>(8)?,
            "colors": row.get::<_, Option<String>>(9)?,
            "matugen": row.get::<_, Option<String>>(10)?,
            "video_file": row.get::<_, Option<String>>(11)?,
            "we_id": row.get::<_, Option<String>>(12)?,
            "analyzed_by": row.get::<_, Option<String>>(13)?,
            "filesize": row.get::<_, Option<i64>>(14)?,
            "width": row.get::<_, Option<i64>>(15)?,
            "height": row.get::<_, Option<i64>>(16)?,
            "mtime": row.get::<_, Option<i64>>(17)?,
            "weather": row.get::<_, Option<String>>(18)?,
        }))
    })?;

    let result: Vec<serde_json::Value> = rows.filter_map(std::result::Result::ok).collect();
    Ok(result)
}

pub fn upsert_cache_entry(
    conn: &Connection,
    key: &str,
    wp_type: &str,
    name: &str,
    thumb: &str,
    thumb_sm: &str,
    video_file: &str,
    we_id: &str,
    mtime: i64,
    hue: i64,
    sat: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO meta(key, type, name, thumb, thumb_sm, video_file, we_id, mtime, hue, sat)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(key) DO UPDATE SET
           type=excluded.type, name=excluded.name, thumb=excluded.thumb,
           thumb_sm=excluded.thumb_sm, video_file=excluded.video_file,
           we_id=excluded.we_id, mtime=excluded.mtime, hue=excluded.hue, sat=excluded.sat",
        params![key, wp_type, name, thumb, thumb_sm, video_file, we_id, mtime, hue, sat],
    )?;
    Ok(())
}

pub fn set_favourite(conn: &Connection, key: &str, favourite: bool) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE meta SET favourite = ?1 WHERE key = ?2",
        params![i64::from(favourite), key],
    )?;
    Ok(changed > 0)
}

pub fn update_analysis(
    conn: &Connection,
    key: &str,
    tags: Option<&str>,
    colors: Option<&str>,
    analyzed_by: Option<&str>,
    hue: Option<i64>,
    sat: Option<i64>,
    weather: Option<&str>,
) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE meta SET tags = COALESCE(?1, tags), tags_raw = COALESCE(?1, tags_raw),
         colors = COALESCE(?2, colors), analyzed_by = COALESCE(?3, analyzed_by),
         hue = COALESCE(?4, hue), sat = COALESCE(?5, sat), weather = COALESCE(?6, weather)
         WHERE key = ?7",
        params![tags, colors, analyzed_by, hue, sat, weather, key],
    )?;
    Ok(changed > 0)
}

pub fn delete_entries(conn: &Connection, keys: &[String]) -> rusqlite::Result<usize> {
    if keys.is_empty() {
        return Ok(0);
    }
    let sql = format!("DELETE FROM meta WHERE key IN ({})", vec!["?"; keys.len()].join(","));
    let params: Vec<&dyn rusqlite::types::ToSql> = keys.iter().map(|k| k as &dyn rusqlite::types::ToSql).collect();
    let deleted = conn.execute(&sql, params.as_slice())?;
    Ok(deleted)
}

pub fn has_entry(conn: &Connection, key: &str) -> bool {
    conn.query_row("SELECT EXISTS(SELECT 1 FROM meta WHERE key = ?1)", params![key], |r| {
        r.get(0)
    })
    .unwrap_or(false)
}

pub fn get_cache_entries(conn: &Connection) -> rusqlite::Result<Vec<(String, String, i64)>> {
    let mut stmt = conn.prepare("SELECT key, type, name, we_id, mtime FROM meta WHERE type IS NOT NULL")?;
    let rows = stmt.query_map([], |row| {
        let wp_type: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
        let name: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
        let we_id: String = row.get::<_, Option<String>>(3)?.unwrap_or_default();
        let mtime: i64 = row.get::<_, Option<i64>>(4)?.unwrap_or(0);
        let cache_key = format!("{}:{}", wp_type, if we_id.is_empty() { &name } else { &we_id });
        Ok((cache_key, row.get::<_, Option<String>>(0)?.unwrap_or_default(), mtime))
    })?;
    rows.collect()
}

pub fn delete_by_name(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    let changed = conn.execute("DELETE FROM meta WHERE name = ?1", params![name])?;
    Ok(changed > 0)
}

pub fn delete_by_name_prefix(conn: &Connection, prefix: &str) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name FROM meta WHERE name LIKE ?1")?;
    let pattern = format!("{}%", prefix.replace('%', "\\%").replace('_', "\\_"));
    let names: Vec<String> = stmt
        .query_map(params![pattern], |row| row.get(0))?
        .filter_map(std::result::Result::ok)
        .collect();
    if !names.is_empty() {
        conn.execute("DELETE FROM meta WHERE name LIKE ?1", params![pattern])?;
    }
    Ok(names)
}

pub fn set_matugen(conn: &Connection, key: &str, matugen_json: &str) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "INSERT INTO meta(key, matugen) VALUES(?1, ?2)
         ON CONFLICT(key) DO UPDATE SET matugen = excluded.matugen",
        params![key, matugen_json],
    )?;
    Ok(changed > 0)
}

pub fn set_matugen_batch(conn: &Connection, entries: &[(String, String)]) -> rusqlite::Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut count = 0;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO meta(key, matugen) VALUES(?1, ?2)
             ON CONFLICT(key) DO UPDATE SET matugen = excluded.matugen",
        )?;
        for (key, matugen_json) in entries {
            count += stmt.execute(params![key, matugen_json])?;
        }
    }
    tx.commit()?;
    Ok(count)
}


pub fn list_image_optimizations(conn: &Connection) -> rusqlite::Result<Vec<(String, String, String)>> {
    let mut stmt = conn.prepare("SELECT src, preset, format FROM image_optimize")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?.unwrap_or_default(),
        ))
    })?;
    rows.collect()
}

pub fn upsert_image_optimize(
    conn: &Connection,
    src: &str,
    dest: &str,
    preset: &str,
    format: &str,
    width: i64,
    height: i64,
    orig_size: i64,
    new_size: i64,
) -> rusqlite::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    conn.execute(
        "INSERT INTO image_optimize(src,dest,preset,format,width,height,orig_size,new_size,optimized_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(src) DO UPDATE SET dest=excluded.dest,preset=excluded.preset,format=excluded.format,
           width=excluded.width,height=excluded.height,orig_size=excluded.orig_size,
           new_size=excluded.new_size,optimized_at=excluded.optimized_at",
        params![src, dest, preset, format, width, height, orig_size, new_size, now],
    )?;
    Ok(())
}

pub fn rename_meta_key(conn: &Connection, old_key: &str, new_key: &str, new_name: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE meta SET key=?1, name=?2 WHERE key=?3",
        params![new_key, new_name, old_key],
    )?;
    Ok(())
}


pub fn list_video_conversions(conn: &Connection) -> rusqlite::Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare("SELECT src, preset FROM video_convert")?;
    let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    rows.collect()
}

pub fn upsert_video_convert(
    conn: &Connection,
    src: &str,
    dest: &str,
    preset: &str,
    codec: &str,
    width: i64,
    height: i64,
    orig_size: i64,
    new_size: i64,
) -> rusqlite::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    conn.execute(
        "INSERT INTO video_convert(src,dest,preset,codec,width,height,orig_size,new_size,converted_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(src) DO UPDATE SET dest=excluded.dest,preset=excluded.preset,codec=excluded.codec,
           width=excluded.width,height=excluded.height,orig_size=excluded.orig_size,
           new_size=excluded.new_size,converted_at=excluded.converted_at",
        params![src, dest, preset, codec, width, height, orig_size, new_size, now],
    )?;
    Ok(())
}

pub fn delete_meta_by_we_id(conn: &Connection, we_id: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM meta WHERE we_id = ?1", params![we_id])?;
    Ok(())
}

pub fn clear_all(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("DELETE FROM meta; DELETE FROM image_optimize; DELETE FROM video_convert;")?;
    info!("cleared all data from database");
    Ok(())
}

#[allow(dead_code)]
pub fn update_meta_dimensions(
    conn: &Connection,
    key: &str,
    filesize: i64,
    width: i64,
    height: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE meta SET filesize=?1, width=?2, height=?3 WHERE key=?4",
        params![filesize, width, height, key],
    )?;
    Ok(())
}
