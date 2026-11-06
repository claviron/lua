//! Pure-Lua packages carried in the binary, so a script has them without
//! installing anything: `require "inspect"`. `require` finds them after the
//! extensions and before `lua.yaml`'s search path.
//!
//! Each is MIT-licensed; its licence sits beside its source in `lua/`.
//! In `lume`, `time` (needs `os`), `trace` (needs `debug`) and `hotswap`
//! (needs `package`) are refused by the sandbox when called; the rest is
//! plain Lua.

/// Name, and the source it loads from.
pub(crate) const PACKAGES: &[(&str, &str)] = &[
    // Tables as readable text, for debugging and logs.
    ("inspect", include_str!("../lua/inspect.lua")),
    // Everyday helpers: map, filter, reduce, merge, split, uuid, …
    ("lume", include_str!("../lua/lume.lua")),
    // Classes and inheritance.
    ("middleclass", include_str!("../lua/middleclass.lua")),
    // Lua values as Lua source, for what JSON cannot carry.
    ("serpent", include_str!("../lua/serpent.lua")),
];

pub(crate) fn source(name: &str) -> Option<&'static str> {
    PACKAGES
        .iter()
        .find(|(package, _)| *package == name)
        .map(|(_, source)| *source)
}
