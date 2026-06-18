use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension, params};
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

#[cfg(test)]
pub(crate) fn open_in_memory() -> rusqlite::Result<Connection> {
    let conn = Connection::open_in_memory()?;
    migrate(&conn)?;
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
        );

        CREATE TABLE IF NOT EXISTS lyrics(
            artist TEXT NOT NULL,
            title TEXT NOT NULL,
            enhanced INTEGER NOT NULL DEFAULT 0,
            data TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (artist, title)
        );",
    )?;

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN tags_raw TEXT", []);

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN analysis_error TEXT", []);

    let _ = conn.execute(
        "ALTER TABLE lyrics ADD COLUMN not_found INTEGER NOT NULL DEFAULT 0",
        [],
    );

    let _ = conn.execute("ALTER TABLE meta ADD COLUMN weather TEXT", []);

    
    let _ = conn.execute("ALTER TABLE meta ADD COLUMN richness INTEGER DEFAULT 0", []);

    
    let _ = conn.execute("ALTER TABLE meta ADD COLUMN apply_count INTEGER DEFAULT 0", []);

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
        anyhow::bail!("already imported - use wall.reimport to force");
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

pub fn random_pick(
    conn: &Connection,
    exclude_name: Option<&str>,
    types: &[&str],
    favourites_only: bool,
) -> rusqlite::Result<Option<(String, String, String, String, String)>> {
    if types.is_empty() {
        return Ok(None);
    }

    let placeholders = std::iter::repeat_n("?", types.len()).collect::<Vec<_>>().join(",");
    let mut sql = format!(
        "SELECT key, type, name, COALESCE(video_file,''), COALESCE(we_id,'') \
         FROM meta WHERE type IN ({placeholders})"
    );
    if favourites_only {
        sql.push_str(" AND favourite = 1");
    }
    if exclude_name.is_some() {
        sql.push_str(" AND name != ?");
    }
    sql.push_str(" ORDER BY RANDOM() LIMIT 1");

    let mut stmt = conn.prepare(&sql)?;
    let mut params_dyn: Vec<&dyn rusqlite::ToSql> =
        types.iter().map(|t| t as &dyn rusqlite::ToSql).collect();
    if let Some(n) = exclude_name.as_ref() {
        params_dyn.push(n);
    }
    stmt.query_row(rusqlite::params_from_iter(params_dyn), |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
        ))
    })
    .optional()
}

pub fn list_wallpapers(conn: &Connection, favourite_only: bool) -> rusqlite::Result<Vec<serde_json::Value>> {
    let sql = if favourite_only {
        "SELECT key, name, type, thumb, thumb_sm, favourite, hue, sat, tags, colors, matugen, video_file, we_id, analyzed_by, filesize, width, height, mtime, weather, richness, apply_count FROM meta WHERE favourite = 1 ORDER BY name"
    } else {
        "SELECT key, name, type, thumb, thumb_sm, favourite, hue, sat, tags, colors, matugen, video_file, we_id, analyzed_by, filesize, width, height, mtime, weather, richness, apply_count FROM meta ORDER BY name"
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
            "richness": row.get::<_, Option<i64>>(19)?,
            "apply_count": row.get::<_, Option<i64>>(20)?,
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
    richness: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO meta(key, type, name, thumb, thumb_sm, video_file, we_id, mtime, hue, sat, richness)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(key) DO UPDATE SET
           type=excluded.type, name=excluded.name, thumb=excluded.thumb,
           thumb_sm=excluded.thumb_sm, video_file=excluded.video_file,
           we_id=excluded.we_id, mtime=excluded.mtime, hue=excluded.hue, sat=excluded.sat,
           richness=excluded.richness",
        params![key, wp_type, name, thumb, thumb_sm, video_file, we_id, mtime, hue, sat, richness],
    )?;
    Ok(())
}

pub fn bump_apply_count(conn: &Connection, key: &str) -> rusqlite::Result<usize> {
    conn.execute(
        "UPDATE meta SET apply_count = COALESCE(apply_count, 0) + 1 WHERE key = ?1",
        params![key],
    )
}

pub fn set_favourite(conn: &Connection, key: &str, favourite: bool) -> rusqlite::Result<bool> {
    let changed = conn.execute(
        "UPDATE meta SET favourite = ?1 WHERE key = ?2",
        params![i64::from(favourite), key],
    )?;
    Ok(changed > 0)
}

pub fn analysis_targets(conn: &Connection) -> rusqlite::Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT key, thumb FROM meta WHERE thumb IS NOT NULL AND thumb != '' ORDER BY key",
    )?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    Ok(rows.flatten().collect())
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

pub fn delete_optimize_by_src(conn: &Connection, src: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM image_optimize WHERE src = ?1", params![src])?;
    conn.execute("DELETE FROM video_convert WHERE src = ?1", params![src])?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn
    }

    fn insert(conn: &Connection, key: &str, wp_type: &str, name: &str) {
        upsert_cache_entry(conn, key, wp_type, name, "thumb", "thumb_sm", "", "", 100, 99, 0, 0).unwrap();
    }

    #[test]
    fn migrate_is_idempotent() {
        let conn = mem();
        assert!(migrate(&conn).is_ok());
    }

    #[test]
    fn analysis_targets_returns_row_keys_and_thumbs_skips_empty() {
        let conn = mem();
        upsert_cache_entry(&conn, "k1", "static", "a", "/c/a.webp", "sm", "", "", 1, 99, 0, 0).unwrap();
        upsert_cache_entry(&conn, "k2", "static", "b", "/c/b.webp", "sm", "", "", 1, 99, 0, 0).unwrap();
        upsert_cache_entry(&conn, "k3", "static", "c", "", "sm", "", "", 1, 99, 0, 0).unwrap();
        let t = analysis_targets(&conn).unwrap();
        assert_eq!(
            t,
            vec![
                ("k1".to_string(), "/c/a.webp".to_string()),
                ("k2".to_string(), "/c/b.webp".to_string()),
            ]
        );
    }

    #[test]
    fn upsert_then_list_and_has_entry() {
        let conn = mem();
        insert(&conn, "static:one", "static", "one");
        assert!(has_entry(&conn, "static:one"));
        assert!(!has_entry(&conn, "missing"));

        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["key"], "static:one");
        assert_eq!(rows[0]["name"], "one");
        assert_eq!(rows[0]["type"], "static");
    }

    #[test]
    fn upsert_updates_in_place_on_conflict() {
        let conn = mem();
        insert(&conn, "k", "static", "old");
        upsert_cache_entry(&conn, "k", "video", "new", "t", "ts", "v.mp4", "", 200, 10, 5, 7).unwrap();
        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "new");
        assert_eq!(rows[0]["type"], "video");
        assert_eq!(rows[0]["video_file"], "v.mp4");
        assert_eq!(rows[0]["richness"], 7);
    }

    #[test]
    fn favourite_filter_and_toggle() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        insert(&conn, "b", "static", "b");
        assert!(set_favourite(&conn, "a", true).unwrap());
        assert!(!set_favourite(&conn, "missing", true).unwrap());

        let favs = list_wallpapers(&conn, true).unwrap();
        assert_eq!(favs.len(), 1);
        assert_eq!(favs[0]["key"], "a");
        assert_eq!(list_wallpapers(&conn, false).unwrap().len(), 2);
    }

    #[test]
    fn bump_apply_count_increments() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        assert_eq!(bump_apply_count(&conn, "a").unwrap(), 1);
        bump_apply_count(&conn, "a").unwrap();
        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows[0]["apply_count"], 2);
    }

    #[test]
    fn get_cache_entries_builds_composite_key() {
        let conn = mem();
        insert(&conn, "static:wall1", "static", "wall1");
        upsert_cache_entry(&conn, "we:99", "we", "scene", "t", "ts", "", "99", 5, 0, 0, 0).unwrap();
        let mut entries = get_cache_entries(&conn).unwrap();
        entries.sort();
        assert_eq!(entries[0], ("static:wall1".to_string(), "static:wall1".to_string(), 100));
        assert_eq!(entries[1], ("we:99".to_string(), "we:99".to_string(), 5));
    }

    #[test]
    fn random_pick_respects_types_exclude_and_favourites() {
        let conn = mem();
        insert(&conn, "s1", "static", "s1");
        insert(&conn, "v1", "video", "v1");

        assert!(random_pick(&conn, None, &[], false).unwrap().is_none());

        let picked = random_pick(&conn, None, &["video"], false).unwrap().unwrap();
        assert_eq!(picked.1, "video");
        assert_eq!(picked.2, "v1");

        assert!(random_pick(&conn, Some("s1"), &["static"], false).unwrap().is_none());
        assert!(random_pick(&conn, None, &["static"], true).unwrap().is_none());

        set_favourite(&conn, "s1", true).unwrap();
        let fav = random_pick(&conn, None, &["static"], true).unwrap().unwrap();
        assert_eq!(fav.0, "s1");
    }

    #[test]
    fn delete_entries_by_keys() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        insert(&conn, "b", "static", "b");
        assert_eq!(delete_entries(&conn, &[]).unwrap(), 0);
        assert_eq!(delete_entries(&conn, &["a".to_string(), "b".to_string()]).unwrap(), 2);
        assert!(list_wallpapers(&conn, false).unwrap().is_empty());
    }

    #[test]
    fn delete_by_name_and_we_id() {
        let conn = mem();
        insert(&conn, "a", "static", "named");
        upsert_cache_entry(&conn, "we:1", "we", "scene", "t", "ts", "", "1", 0, 0, 0, 0).unwrap();
        assert!(delete_by_name(&conn, "named").unwrap());
        assert!(!delete_by_name(&conn, "named").unwrap());
        delete_meta_by_we_id(&conn, "1").unwrap();
        assert!(list_wallpapers(&conn, false).unwrap().is_empty());
    }

    #[test]
    fn delete_by_name_prefix_returns_matched_names() {
        let conn = mem();
        insert(&conn, "a", "static", "trip01");
        insert(&conn, "b", "static", "trip02");
        insert(&conn, "c", "static", "other");
        let mut deleted = delete_by_name_prefix(&conn, "trip").unwrap();
        deleted.sort();
        assert_eq!(deleted, vec!["trip01".to_string(), "trip02".to_string()]);
        let remaining = list_wallpapers(&conn, false).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0]["name"], "other");
    }

    #[test]
    fn rename_meta_key_changes_key_and_name() {
        let conn = mem();
        insert(&conn, "old", "static", "oldname");
        rename_meta_key(&conn, "old", "new", "newname").unwrap();
        assert!(!has_entry(&conn, "old"));
        assert!(has_entry(&conn, "new"));
        assert_eq!(list_wallpapers(&conn, false).unwrap()[0]["name"], "newname");
    }

    #[test]
    fn update_analysis_coalesces_none_fields() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        assert!(update_analysis(&conn, "a", Some("tag1,tag2"), None, Some("ollama"), Some(120), None, None).unwrap());
        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows[0]["tags"], "tag1,tag2");
        assert_eq!(rows[0]["analyzed_by"], "ollama");
        assert_eq!(rows[0]["hue"], 120);
        assert_eq!(rows[0]["sat"], 0);

        update_analysis(&conn, "a", None, Some("#fff"), None, None, None, None).unwrap();
        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows[0]["tags"], "tag1,tag2");
        assert_eq!(rows[0]["colors"], "#fff");
    }

    #[test]
    fn update_meta_dimensions_sets_size_and_geometry() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        update_meta_dimensions(&conn, "a", 4096, 1920, 1080).unwrap();
        let rows = list_wallpapers(&conn, false).unwrap();
        assert_eq!(rows[0]["filesize"], 4096);
        assert_eq!(rows[0]["width"], 1920);
        assert_eq!(rows[0]["height"], 1080);
    }

    #[test]
    fn image_optimize_roundtrip_and_delete() {
        let conn = mem();
        upsert_image_optimize(&conn, "src.png", "dest.webp", "balanced", "webp", 800, 600, 1000, 400).unwrap();
        let list = list_image_optimizations(&conn).unwrap();
        assert_eq!(list, vec![("src.png".to_string(), "balanced".to_string(), "webp".to_string())]);
        delete_optimize_by_src(&conn, "src.png").unwrap();
        assert!(list_image_optimizations(&conn).unwrap().is_empty());
    }

    #[test]
    fn video_convert_roundtrip() {
        let conn = mem();
        upsert_video_convert(&conn, "in.mov", "out.mp4", "h264", "h264", 1920, 1080, 9999, 5000).unwrap();
        let list = list_video_conversions(&conn).unwrap();
        assert_eq!(list, vec![("in.mov".to_string(), "h264".to_string())]);
    }

    #[test]
    fn clear_all_empties_tables() {
        let conn = mem();
        insert(&conn, "a", "static", "a");
        upsert_image_optimize(&conn, "s", "d", "p", "webp", 1, 1, 1, 1).unwrap();
        upsert_video_convert(&conn, "s", "d", "p", "c", 1, 1, 1, 1).unwrap();
        clear_all(&conn).unwrap();
        assert!(list_wallpapers(&conn, false).unwrap().is_empty());
        assert!(list_image_optimizations(&conn).unwrap().is_empty());
        assert!(list_video_conversions(&conn).unwrap().is_empty());
    }
}
