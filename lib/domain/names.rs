/// Normalized form of a project or tag name: surrounding whitespace removed, then Unicode case folded.
/// Upper case then lower case gives full case folding with the standard library (for example "Straße" and "STRASSE" match).
pub fn normalize(name: &str) -> String {
    name.trim().to_uppercase().to_lowercase()
}
