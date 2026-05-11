use std::path::PathBuf;

const FONT_CANDIDATES: &[&str] = &[
    "/usr/share/fonts/TTF/MaterialDesignIconsDesktop.ttf",
    "/usr/share/fonts/truetype/materialdesignicons-desktop/MaterialDesignIconsDesktop.ttf",
];

pub fn run(args: &[String]) -> i32 {
    let mut font_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--font" => {
                font_path = iter.next().map(PathBuf::from);
            }
            "--output" | "-o" => {
                output_path = iter.next().map(PathBuf::from);
            }
            "-h" | "--help" => {
                println!("usage: skwd gen-icons [--font PATH] [--output PATH]");
                println!("Generates an MDI icon cache (name -> glyph) JSON file.");
                return 0;
            }
            other => {
                eprintln!("unknown arg: {other}");
                return 1;
            }
        }
    }

    let font_path = match font_path.or_else(find_mdi_font) {
        Some(p) => p,
        None => {
            eprintln!("could not find MaterialDesignIconsDesktop.ttf");
            eprintln!("install ttf-material-design-icons-desktop or pass --font PATH");
            return 1;
        }
    };

    let output_path = output_path.unwrap_or_else(default_output_path);

    let data = match std::fs::read(&font_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("read {}: {e}", font_path.display());
            return 1;
        }
    };

    let face = match ttf_parser::Face::parse(&data, 0) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("parse {}: {e}", font_path.display());
            return 1;
        }
    };

    let names = match read_post2_names(&data) {
        Some(n) => n,
        None => {
            eprintln!("font has no post 2.0 table with glyph names");
            return 1;
        }
    };

    let mut entries: Vec<serde_json::Value> = Vec::new();
    for cp_u32 in 0xF0001u32..=0xF1FFFu32 {
        let Some(ch) = char::from_u32(cp_u32) else { continue };
        let Some(gid) = face.glyph_index(ch) else { continue };
        let Some(raw) = names.get(gid.0 as usize) else { continue };
        if raw.is_empty() || raw == ".notdef" {
            continue;
        }
        let display = raw.replace(['_', '-'], " ");
        entries.push(serde_json::json!({ "n": display, "g": ch.to_string() }));
    }

    if let Some(parent) = output_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!("create_dir_all {}: {e}", parent.display());
            return 1;
        }
    }

    let serialized = match serde_json::to_string(&entries) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("serialize: {e}");
            return 1;
        }
    };

    if let Err(e) = std::fs::write(&output_path, serialized) {
        eprintln!("write {}: {e}", output_path.display());
        return 1;
    }

    println!(
        "wrote {} icons -> {}",
        entries.len(),
        output_path.display()
    );
    0
}

fn read_post2_names(data: &[u8]) -> Option<Vec<String>> {
    if data.len() < 12 {
        return None;
    }
    let num_tables = u16::from_be_bytes([data[4], data[5]]) as usize;
    let mut post_offset = 0usize;
    let mut post_len = 0usize;
    let mut found = false;
    for i in 0..num_tables {
        let dir = 12 + i * 16;
        if dir + 16 > data.len() {
            return None;
        }
        if &data[dir..dir + 4] == b"post" {
            post_offset =
                u32::from_be_bytes([data[dir + 8], data[dir + 9], data[dir + 10], data[dir + 11]])
                    as usize;
            post_len =
                u32::from_be_bytes([data[dir + 12], data[dir + 13], data[dir + 14], data[dir + 15]])
                    as usize;
            found = true;
            break;
        }
    }
    if !found || post_offset + 34 > data.len() {
        return None;
    }
    let post = &data[post_offset..post_offset + post_len.min(data.len() - post_offset)];
    let version = u32::from_be_bytes([post[0], post[1], post[2], post[3]]);
    if version != 0x00020000 || post.len() < 34 {
        return None;
    }
    let num_glyphs = u16::from_be_bytes([post[32], post[33]]) as usize;
    let indices_end = 34 + num_glyphs * 2;
    if indices_end > post.len() {
        return None;
    }
    let mut indices = Vec::with_capacity(num_glyphs);
    for i in 0..num_glyphs {
        let o = 34 + i * 2;
        indices.push(u16::from_be_bytes([post[o], post[o + 1]]));
    }
    let mut strings: Vec<String> = Vec::new();
    let mut pos = indices_end;
    while pos < post.len() {
        let n = post[pos] as usize;
        pos += 1;
        if pos + n > post.len() {
            break;
        }
        strings.push(String::from_utf8_lossy(&post[pos..pos + n]).into_owned());
        pos += n;
    }
    let mut names = Vec::with_capacity(num_glyphs);
    for (gid, idx) in indices.iter().enumerate() {
        let idx = *idx as usize;
        let name = if idx < 258 {
            mac_standard_name(idx)
                .map(String::from)
                .unwrap_or_else(|| format!("glyph{gid}"))
        } else {
            let s_idx = idx - 258;
            strings
                .get(s_idx)
                .cloned()
                .unwrap_or_else(|| format!("glyph{gid}"))
        };
        names.push(name);
    }
    Some(names)
}

fn mac_standard_name(idx: usize) -> Option<&'static str> {
    const MAC_NAMES: &[&str] = &[
        ".notdef", ".null", "nonmarkingreturn", "space", "exclam", "quotedbl", "numbersign",
        "dollar", "percent", "ampersand", "quotesingle", "parenleft", "parenright", "asterisk",
        "plus", "comma", "hyphen", "period", "slash", "zero", "one", "two", "three", "four",
        "five", "six", "seven", "eight", "nine", "colon", "semicolon", "less", "equal", "greater",
        "question", "at", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N",
        "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "bracketleft", "backslash",
        "bracketright", "asciicircum", "underscore", "grave", "a", "b", "c", "d", "e", "f", "g",
        "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y",
        "z", "braceleft", "bar", "braceright", "asciitilde",
    ];
    MAC_NAMES.get(idx).copied()
}

fn find_mdi_font() -> Option<PathBuf> {
    for c in FONT_CANDIDATES {
        let p = PathBuf::from(c);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let p = PathBuf::from(home).join(".local/share/fonts/MaterialDesignIconsDesktop.ttf");
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn default_output_path() -> PathBuf {
    let config_dir = std::env::var_os("SKWD_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let base = std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
                .unwrap_or_else(|| PathBuf::from("."));
            base.join("skwd")
        });
    config_dir.join("data/mdi-icons.json")
}
