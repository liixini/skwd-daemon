use image::DynamicImage;
use serde_json::Value;

pub struct EffectDef {
    pub id: &'static str,
    pub schema: fn() -> Value,
    pub render: fn(DynamicImage, &Value) -> anyhow::Result<DynamicImage>,
    pub suffix: Option<fn(&Value) -> String>,
}

fn categorized() -> Vec<(&'static str, Vec<EffectDef>)> {
    vec![
        ("Colour", super::color::effects()),
        ("Tone", super::tone::effects()),
        ("Stylize", super::stylize::effects()),
        ("Distort", super::distort::effects()),
    ]
}

#[must_use]
pub fn all() -> Vec<EffectDef> {
    categorized().into_iter().flat_map(|(_, v)| v).collect()
}

#[must_use]
pub fn find(id: &str) -> Option<EffectDef> {
    all().into_iter().find(|e| e.id == id)
}

#[must_use]
pub fn schemas() -> Vec<Value> {
    categorized()
        .into_iter()
        .flat_map(|(cat, defs)| {
            defs.into_iter().map(move |e| {
                let mut s = (e.schema)();
                s["category"] = Value::String(cat.to_string());
                s
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::RgbaImage;

    fn default_params(schema: &Value) -> Value {
        let mut map = serde_json::Map::new();
        if let Some(ps) = schema.get("params").and_then(|p| p.as_array()) {
            for p in ps {
                if let (Some(id), Some(def)) =
                    (p.get("id").and_then(|i| i.as_str()), p.get("default"))
                {
                    map.insert(id.to_string(), def.clone());
                }
            }
        }
        Value::Object(map)
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn every_registered_effect_renders_without_panic() {
        let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(24, 18, |x, y| {
            image::Rgba([(x * 9) as u8, (y * 11) as u8, ((x + y) * 5) as u8, 255])
        }));
        let defs = all();
        assert!(defs.len() >= 20, "expected the full effect set, got {}", defs.len());
        for def in defs {
            let schema = (def.schema)();
            assert_eq!(
                schema.get("id").and_then(|i| i.as_str()),
                Some(def.id),
                "schema id mismatch for {}",
                def.id
            );
            let params = default_params(&schema);
            let out = (def.render)(img.clone(), &params)
                .unwrap_or_else(|e| panic!("effect {} failed: {e}", def.id));
            assert!(
                out.width() > 0 && out.height() > 0,
                "effect {} produced an empty image",
                def.id
            );
        }
    }

    #[test]
    fn ids_are_unique() {
        let mut ids: Vec<&str> = all().iter().map(|e| e.id).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "duplicate effect ids registered");
    }
}
