//! Code-aware chunking (CODE-001).
//!
//! Fixed-size chunking is destructive for code: it splits signatures from bodies
//! and tests from sources. This module instead chunks source by *top-level
//! symbol* (functions, types, classes), keeping each definition intact along with
//! its immediately-preceding doc comments and attributes.
//!
//! It is a deliberately dependency-free heuristic: Rust blocks are found by brace
//! matching, Python blocks by indentation. This avoids pulling in tree-sitter's
//! compiled C grammars. A tree-sitter backend (precise spans, nested symbols,
//! call/import edges) is the documented future upgrade behind this same API.
//! Known limitation: braces inside strings/comments are not specially handled.

/// Source language for chunking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Rust,
    Python,
    Other,
}

impl Language {
    /// Map a file extension (without dot) to a language.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "rs" => Language::Rust,
            "py" => Language::Python,
            _ => Language::Other,
        }
    }
}

/// The kind of a code symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolKind {
    Function,
    Struct,
    Enum,
    Trait,
    Impl,
    Module,
    Const,
    Static,
    TypeAlias,
    Macro,
    Class,
    Other,
}

/// One code chunk: a top-level symbol with its source span (1-based lines).
#[derive(Debug, Clone, PartialEq)]
pub struct CodeChunk {
    pub kind: SymbolKind,
    pub name: Option<String>,
    pub text: String,
    pub start_line: usize,
    pub end_line: usize,
}

/// Chunk source code into top-level symbol chunks.
///
/// For [`Language::Other`], returns a single whole-source chunk (or none if the
/// source is blank).
pub fn chunk_code(source: &str, lang: Language) -> Vec<CodeChunk> {
    match lang {
        Language::Rust => chunk_rust(source),
        Language::Python => chunk_python(source),
        Language::Other => {
            if source.trim().is_empty() {
                Vec::new()
            } else {
                let line_count = source.lines().count().max(1);
                vec![CodeChunk {
                    kind: SymbolKind::Other,
                    name: None,
                    text: source.to_string(),
                    start_line: 1,
                    end_line: line_count,
                }]
            }
        }
    }
}

fn leading_indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// Walk backward from `start` over contiguous attribute / doc-comment / comment
/// lines so they are attached to the symbol below them.
fn extend_start_with_decorations(lines: &[&str], start: usize) -> usize {
    let mut s = start;
    while s > 0 {
        let prev = lines[s - 1].trim_start();
        let is_decoration = prev.starts_with("#[")
            || prev.starts_with("#!")
            || prev.starts_with("///")
            || prev.starts_with("//!")
            || prev.starts_with("//")
            || prev.starts_with('@'); // Python decorator
        if is_decoration {
            s -= 1;
        } else {
            break;
        }
    }
    s
}

fn rust_symbol_start(trimmed: &str) -> Option<SymbolKind> {
    let t = strip_rust_item_prefixes(trimmed);
    let kind = if t.starts_with("fn ") {
        SymbolKind::Function
    } else if t.starts_with("struct ") {
        SymbolKind::Struct
    } else if t.starts_with("enum ") {
        SymbolKind::Enum
    } else if t.starts_with("trait ") {
        SymbolKind::Trait
    } else if t == "impl" || t.starts_with("impl ") || t.starts_with("impl<") {
        SymbolKind::Impl
    } else if t.starts_with("mod ") {
        SymbolKind::Module
    } else if t.starts_with("const ") {
        SymbolKind::Const
    } else if t.starts_with("static ") {
        SymbolKind::Static
    } else if t.starts_with("type ") {
        SymbolKind::TypeAlias
    } else if t.starts_with("macro_rules! ") || t.starts_with("macro ") {
        SymbolKind::Macro
    } else {
        return None;
    };
    Some(kind)
}

fn rust_symbol_name(trimmed: &str, kind: SymbolKind) -> Option<String> {
    let t = strip_rust_item_prefixes(trimmed);
    let keyword = match kind {
        SymbolKind::Function => "fn ",
        SymbolKind::Struct => "struct ",
        SymbolKind::Enum => "enum ",
        SymbolKind::Trait => "trait ",
        SymbolKind::Module => "mod ",
        SymbolKind::Impl => return impl_name(t),
        SymbolKind::Const => "const ",
        SymbolKind::Static => return static_name(t),
        SymbolKind::TypeAlias => "type ",
        SymbolKind::Macro => return macro_name(t),
        _ => return None,
    };
    let rest = t.strip_prefix(keyword)?;
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn strip_rust_item_prefixes(mut t: &str) -> &str {
    loop {
        let before = t;
        if let Some(rest) = strip_rust_visibility(t) {
            t = rest;
        } else if let Some(rest) = strip_rust_word_prefix(t, "async") {
            t = rest;
        } else if let Some(rest) = strip_rust_word_prefix(t, "unsafe") {
            t = rest;
        } else if let Some(rest) = strip_rust_word_prefix(t, "default") {
            t = rest;
        } else if let Some(rest) = strip_rust_const_fn_prefix(t) {
            t = rest;
        } else if let Some(rest) = strip_rust_extern_prefix(t) {
            t = rest;
        }

        if t == before {
            return t;
        }
    }
}

fn strip_rust_visibility(t: &str) -> Option<&str> {
    let rest = t.strip_prefix("pub")?;
    match rest.chars().next() {
        Some(c) if c.is_whitespace() => Some(rest.trim_start()),
        Some('(') => {
            let end = rest.find(')')?;
            Some(rest[end + 1..].trim_start())
        }
        _ => None,
    }
}

fn strip_rust_word_prefix<'a>(t: &'a str, word: &str) -> Option<&'a str> {
    let rest = t.strip_prefix(word)?;
    match rest.chars().next() {
        Some(c) if c.is_whitespace() => Some(rest.trim_start()),
        _ => None,
    }
}

fn strip_rust_const_fn_prefix(t: &str) -> Option<&str> {
    let rest = strip_rust_word_prefix(t, "const")?;
    if rest.starts_with("fn ") {
        Some(rest)
    } else {
        None
    }
}

fn strip_rust_extern_prefix(t: &str) -> Option<&str> {
    let rest = strip_rust_word_prefix(t, "extern")?;
    if let Some(abi) = rest.strip_prefix('"') {
        let end = abi.find('"')?;
        Some(abi[end + 1..].trim_start())
    } else {
        Some(rest)
    }
}

fn static_name(t: &str) -> Option<String> {
    let rest = t.strip_prefix("static ")?.trim_start();
    let rest = rest.strip_prefix("mut ").unwrap_or(rest);
    rust_identifier_prefix(rest)
}

fn macro_name(t: &str) -> Option<String> {
    let rest = t
        .strip_prefix("macro_rules! ")
        .or_else(|| t.strip_prefix("macro "))?;
    rust_identifier_prefix(rest.trim_start())
}

fn rust_identifier_prefix(s: &str) -> Option<String> {
    let name: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Best-effort name for an impl block: the implemented type (after `for`, else
/// the trait/type token).
fn impl_name(t: &str) -> Option<String> {
    let body = t.strip_prefix("impl").unwrap_or(t).trim_start();
    // Drop generic params like `<T>` right after impl.
    let body = if let Some(stripped) = body.strip_prefix('<') {
        // skip to matching '>' (shallow)
        match stripped.find('>') {
            Some(idx) => stripped[idx + 1..].trim_start(),
            None => body,
        }
    } else {
        body
    };
    let target = if let Some(idx) = body.find(" for ") {
        &body[idx + 5..]
    } else {
        body
    };
    let name: String = target
        .trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Find the line index where a Rust item starting at `start` ends.
fn rust_block_end(lines: &[&str], start: usize) -> usize {
    let mut depth: i32 = 0;
    let mut opened = false;
    let mut in_block_comment = false;
    for (offset, line) in lines[start..].iter().enumerate() {
        let scan = rust_brace_scan_with_state(line, &mut in_block_comment);
        if scan.saw_open {
            opened = true;
        }
        depth += scan.delta;
        if opened && depth <= 0 {
            return start + offset;
        }
        // One-liner item (no body): terminated by ';' before any '{'.
        if !opened && line.contains(';') {
            return start + offset;
        }
    }
    lines.len() - 1
}

#[derive(Debug, Clone, Copy)]
struct RustBraceScan {
    delta: i32,
    saw_open: bool,
}

fn rust_brace_scan_with_state(line: &str, in_block_comment: &mut bool) -> RustBraceScan {
    let chars: Vec<char> = line.chars().collect();
    let mut delta = 0;
    let mut saw_open = false;
    let mut i = 0;

    while i < chars.len() {
        if *in_block_comment {
            if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                *in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        match chars[i] {
            '/' if chars.get(i + 1) == Some(&'/') => break,
            '/' if chars.get(i + 1) == Some(&'*') => {
                *in_block_comment = true;
                i += 1;
            }
            'r' => {
                if let Some(end) = rust_raw_string_end(&chars, i) {
                    i = end;
                }
            }
            'b' if chars.get(i + 1) == Some(&'r') => {
                if let Some(end) = rust_raw_string_end(&chars, i + 1) {
                    i = end;
                }
            }
            '"' => {
                i = skip_rust_string(&chars, i);
            }
            '\'' => {
                if let Some(end) = rust_char_literal_end(&chars, i) {
                    i = end;
                }
            }
            '{' => {
                saw_open = true;
                delta += 1;
            }
            '}' => delta -= 1,
            _ => {}
        }
        i += 1;
    }

    RustBraceScan { delta, saw_open }
}

fn rust_raw_string_end(chars: &[char], start: usize) -> Option<usize> {
    let mut i = start + 1;
    let mut hashes = 0usize;

    while chars.get(i) == Some(&'#') {
        hashes += 1;
        i += 1;
    }

    if chars.get(i) != Some(&'"') {
        return None;
    }

    i += 1;
    while i < chars.len() {
        if chars[i] == '"' {
            let mut matched = true;
            for offset in 0..hashes {
                if chars.get(i + 1 + offset) != Some(&'#') {
                    matched = false;
                    break;
                }
            }
            if matched {
                return Some(i + hashes);
            }
        }
        i += 1;
    }

    Some(chars.len().saturating_sub(1))
}

fn skip_rust_string(chars: &[char], start: usize) -> usize {
    let mut i = start + 1;
    let mut escaped = false;
    while i < chars.len() {
        let ch = chars[i];
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return i;
        }
        i += 1;
    }
    chars.len().saturating_sub(1)
}

fn rust_char_literal_end(chars: &[char], start: usize) -> Option<usize> {
    let mut i = start + 1;
    if i >= chars.len() {
        return None;
    }

    if chars[i] == '\\' {
        i += 1;
        if i >= chars.len() {
            return None;
        }
        if chars[i] == 'u' && chars.get(i + 1) == Some(&'{') {
            i += 2;
            while i < chars.len() && chars[i] != '}' {
                i += 1;
            }
            if i >= chars.len() {
                return None;
            }
        }
    }

    i += 1;
    if chars.get(i) == Some(&'\'') {
        Some(i)
    } else {
        None
    }
}

fn chunk_rust(source: &str) -> Vec<CodeChunk> {
    let lines: Vec<&str> = source.lines().collect();
    let mut chunks = Vec::new();
    let mut depth: i32 = 0;
    let mut in_block_comment = false;
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        if depth == 0 {
            if let Some(kind) = rust_symbol_start(trimmed) {
                let name = rust_symbol_name(trimmed, kind);
                let end = rust_block_end(&lines, i);
                let chunk_start = extend_start_with_decorations(&lines, i);
                chunks.push(CodeChunk {
                    kind,
                    name,
                    text: lines[chunk_start..=end].join("\n"),
                    start_line: chunk_start + 1,
                    end_line: end + 1,
                });
                i = end + 1;
                continue;
            }
        }
        depth += rust_brace_scan_with_state(lines[i], &mut in_block_comment).delta;
        i += 1;
    }
    chunks
}

fn chunk_python(source: &str) -> Vec<CodeChunk> {
    let lines: Vec<&str> = source.lines().collect();
    let mut chunks = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim_start();
        // Only top-level (column 0) defs/classes; methods stay within their class.
        let is_top_level = leading_indent(line) == 0 && !trimmed.is_empty();
        let kind = if is_top_level {
            if trimmed.starts_with("def ") || trimmed.starts_with("async def ") {
                Some(SymbolKind::Function)
            } else if trimmed.starts_with("class ") {
                Some(SymbolKind::Class)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(kind) = kind {
            let end = python_block_end(&lines, i);
            let name = python_name(trimmed, kind);
            let chunk_start = extend_start_with_decorations(&lines, i);
            chunks.push(CodeChunk {
                kind,
                name,
                text: lines[chunk_start..=end].join("\n"),
                start_line: chunk_start + 1,
                end_line: end + 1,
            });
            i = end + 1;
            continue;
        }
        i += 1;
    }
    chunks
}

fn python_block_end(lines: &[&str], start: usize) -> usize {
    let mut end = start;
    let mut header_depth = 0i32;
    let mut header_complete = python_header_complete(lines[start], &mut header_depth);
    let mut j = start + 1;

    while j < lines.len() {
        let trimmed = lines[j].trim();
        if trimmed.is_empty() {
            j += 1;
            continue;
        }

        if !header_complete {
            end = j;
            header_complete = python_header_complete(trimmed, &mut header_depth);
            j += 1;
            continue;
        }

        if leading_indent(lines[j]) == 0 {
            break;
        }

        end = j;
        j += 1;
    }

    end
}

fn python_header_complete(line: &str, depth: &mut i32) -> bool {
    for ch in line.chars() {
        match ch {
            '(' | '[' | '{' => *depth += 1,
            ')' | ']' | '}' => *depth = (*depth - 1).max(0),
            ':' if *depth == 0 => return true,
            '#' if *depth == 0 => break,
            _ => {}
        }
    }
    false
}

fn python_name(trimmed: &str, kind: SymbolKind) -> Option<String> {
    let rest = match kind {
        SymbolKind::Function => trimmed
            .strip_prefix("async def ")
            .or_else(|| trimmed.strip_prefix("def "))?,
        SymbolKind::Class => trimmed.strip_prefix("class ")?,
        _ => return None,
    };
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_from_extension() {
        assert_eq!(Language::from_extension("rs"), Language::Rust);
        assert_eq!(Language::from_extension("PY"), Language::Python);
        assert_eq!(Language::from_extension("txt"), Language::Other);
    }

    #[test]
    fn test_rust_extracts_top_level_symbols() {
        let src = "\
use std::fmt;

/// Adds two numbers.
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

struct Point {
    x: i32,
    y: i32,
}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].kind, SymbolKind::Function);
        assert_eq!(chunks[0].name.as_deref(), Some("add"));
        // doc comment is attached to the function chunk
        assert!(chunks[0].text.contains("/// Adds two numbers."));
        assert!(chunks[0].text.contains("a + b"));
        assert_eq!(chunks[1].kind, SymbolKind::Struct);
        assert_eq!(chunks[1].name.as_deref(), Some("Point"));
        assert!(chunks[1].text.contains("x: i32"));
    }

    #[test]
    fn test_rust_function_body_not_split() {
        let src = "\
fn outer() {
    let x = 1;
    {
        let y = 2;
    }
}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 1);
        assert!(
            chunks[0].text.contains("let y = 2;"),
            "nested block kept in body"
        );
        assert_eq!(chunks[0].end_line, 6);
    }

    #[test]
    fn test_rust_braces_inside_strings_do_not_break_top_level_scanning() {
        let src = "\
fn first() {
    let pattern = \"}\";
}

fn second() {
    let pattern = \"{\";
}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert_eq!(chunks[0].end_line, 3);
        assert_eq!(chunks[1].start_line, 5);
    }

    #[test]
    fn test_rust_braces_inside_block_comments_do_not_break_scanning() {
        let src = "\
fn first() {
    /*
     * comment contains }
     */
    let value = 1;
}

fn second() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert!(chunks[0].text.contains("let value = 1;"));
        assert_eq!(chunks[1].start_line, 8);
    }

    #[test]
    fn test_rust_braces_inside_raw_strings_do_not_break_scanning() {
        let src = "\
fn first() {
    let template = r#\"inner \" then } still text\"#;
}

fn second() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert_eq!(chunks[0].end_line, 3);
        assert_eq!(chunks[1].start_line, 5);
    }

    #[test]
    fn test_rust_one_line_functions_are_separate_chunks() {
        let src = "\
fn first() {}
fn second() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert_eq!(chunks[0].start_line, 1);
        assert_eq!(chunks[0].end_line, 1);
        assert_eq!(chunks[1].start_line, 2);
        assert_eq!(chunks[1].end_line, 2);
    }

    #[test]
    fn test_rust_modifier_chains_are_chunked() {
        let src = "\
pub(in crate::api) async fn fetch_data() {
    do_work().await;
}

pub extern \"C\" fn reset_state() {}

pub(crate) const fn stable_id() -> u64 {
    42
}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["fetch_data", "reset_state", "stable_id"]);
    }

    #[test]
    fn test_rust_extracts_top_level_constants_types_and_macros() {
        let src = "\
pub const DEFAULT_BATCH_SIZE: usize = 128;
static mut LAST_ERROR: Option<String> = None;
type SchedulerId = u64;

macro_rules! trace_query {
    ($query:expr) => {
        println!(\"{}\", $query);
    };
}

pub fn search() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(
            names,
            vec![
                "DEFAULT_BATCH_SIZE",
                "LAST_ERROR",
                "SchedulerId",
                "trace_query",
                "search"
            ]
        );
        assert_eq!(chunks[0].kind, SymbolKind::Const);
        assert_eq!(chunks[1].kind, SymbolKind::Static);
        assert_eq!(chunks[2].kind, SymbolKind::TypeAlias);
        assert_eq!(chunks[3].kind, SymbolKind::Macro);
        assert!(chunks[3].text.contains("println!"));
    }

    #[test]
    fn test_rust_impl_and_attributes() {
        let src = "\
#[derive(Debug)]
pub struct Foo;

impl Foo {
    fn bar(&self) {}
}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].kind, SymbolKind::Struct);
        assert!(
            chunks[0].text.contains("#[derive(Debug)]"),
            "attribute attached"
        );
        assert_eq!(chunks[1].kind, SymbolKind::Impl);
        assert_eq!(chunks[1].name.as_deref(), Some("Foo"));
    }

    #[test]
    fn test_rust_impl_trait_for_type_name() {
        let src = "impl fmt::Display for Point {\n    fn fmt(&self) {}\n}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks[0].kind, SymbolKind::Impl);
        assert_eq!(chunks[0].name.as_deref(), Some("Point"));
    }

    #[test]
    fn test_python_extracts_functions_and_classes() {
        let src = "\
import os

def top_level():
    return 1

class Greeter:
    def __init__(self, name):
        self.name = name

    def greet(self):
        return self.name
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].kind, SymbolKind::Function);
        assert_eq!(chunks[0].name.as_deref(), Some("top_level"));
        assert_eq!(chunks[1].kind, SymbolKind::Class);
        assert_eq!(chunks[1].name.as_deref(), Some("Greeter"));
        // methods stay inside the class chunk
        assert!(chunks[1].text.contains("def greet"));
    }

    #[test]
    fn test_python_decorator_attached() {
        let src = "\
@decorator
def decorated():
    pass
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.contains("@decorator"));
        assert_eq!(chunks[0].name.as_deref(), Some("decorated"));
    }

    #[test]
    fn test_python_multiline_signature_keeps_body() {
        let src = "\
def build_context(
    query,
    passages,
):
    return query, passages

def next_symbol():
    return None
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].name.as_deref(), Some("build_context"));
        assert!(chunks[0].text.contains("passages,"));
        assert!(chunks[0].text.contains("return query, passages"));
        assert_eq!(chunks[0].end_line, 5);
        assert_eq!(chunks[1].name.as_deref(), Some("next_symbol"));
        assert_eq!(chunks[1].start_line, 7);
    }

    #[test]
    fn test_other_language_single_chunk() {
        let chunks = chunk_code("hello\nworld", Language::Other);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].kind, SymbolKind::Other);
        assert_eq!(chunks[0].end_line, 2);
        assert!(chunk_code("   ", Language::Other).is_empty());
    }
}
