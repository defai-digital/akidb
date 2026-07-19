//! Code-aware chunking (CODE-001).
//!
//! Fixed-size chunking is destructive for code: it splits signatures from bodies
//! and tests from sources. This module instead chunks source by *top-level
//! symbol* (functions, types, classes), keeping each definition intact along with
//! its immediately-preceding doc comments and attributes.
//!
//! Dependency-free language scanners for Rust, Python, TypeScript/JavaScript,
//! and Go. Blocks use brace matching (or Python indentation). Spans are stable
//! line + byte ranges suitable for graph edges and citations. Known limitation:
//! not a full tree-sitter parse of nested / macro-heavy code.

/// Source language for chunking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Rust,
    Python,
    TypeScript,
    JavaScript,
    Go,
    Other,
}

impl Language {
    /// Map a file extension (without dot) to a language.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "rs" => Language::Rust,
            "py" => Language::Python,
            "ts" | "tsx" => Language::TypeScript,
            "js" | "jsx" | "mjs" | "cjs" => Language::JavaScript,
            "go" => Language::Go,
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

/// One code chunk: a top-level symbol with its source span (1-based lines + bytes).
#[derive(Debug, Clone, PartialEq)]
pub struct CodeChunk {
    pub kind: SymbolKind,
    pub name: Option<String>,
    pub text: String,
    pub start_line: usize,
    pub end_line: usize,
    /// Inclusive byte offset of the first character of `start_line` in source.
    pub start_byte: usize,
    /// Exclusive byte offset after the last character of `end_line` in source.
    pub end_byte: usize,
}

/// Edge kinds emitted for code graph expansion (calls / imports / tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeEdgeKind {
    Imports,
    Calls,
    TestedBy,
}

/// A lightweight code graph edge: source symbol → target name (or path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeEdge {
    pub kind: CodeEdgeKind,
    pub from_symbol: Option<String>,
    pub to_symbol: String,
}

/// Chunk source code into top-level symbol chunks.
///
/// For [`Language::Other`], returns a single whole-source chunk (or none if the
/// source is blank).
pub fn chunk_code(source: &str, lang: Language) -> Vec<CodeChunk> {
    match lang {
        Language::Rust => chunk_rust(source),
        Language::Python => chunk_python(source),
        Language::TypeScript | Language::JavaScript => chunk_js_family(source),
        Language::Go => chunk_go(source),
        Language::Other => {
            if source.trim().is_empty() {
                Vec::new()
            } else {
                let line_count = source.lines().count().max(1);
                let (start_byte, end_byte) = line_byte_range(source, 1, line_count);
                vec![CodeChunk {
                    kind: SymbolKind::Other,
                    name: None,
                    text: source.to_string(),
                    start_line: 1,
                    end_line: line_count,
                    start_byte,
                    end_byte,
                }]
            }
        }
    }
}

/// Extract import / call / tested_by edges from a source unit for graph indexing.
pub fn extract_code_edges(source: &str, lang: Language, from_symbol: Option<&str>) -> Vec<CodeEdge> {
    let mut edges = Vec::new();
    match lang {
        Language::Rust => {
            for line in source.lines() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("use ") {
                    let target = rest
                        .trim_end_matches(';')
                        .split(" as ")
                        .next()
                        .unwrap_or(rest)
                        .trim()
                        .to_string();
                    if !target.is_empty() {
                        edges.push(CodeEdge {
                            kind: CodeEdgeKind::Imports,
                            from_symbol: from_symbol.map(str::to_string),
                            to_symbol: target,
                        });
                    }
                }
                // Heuristic call sites: foo( or Foo::bar(
                for token in t.split(|c: char| !c.is_alphanumeric() && c != '_' && c != ':') {
                    if token.contains("::") {
                        let name = token.split("::").last().unwrap_or(token);
                        if !name.is_empty() && name.chars().next().is_some_and(|c| c.is_lowercase())
                        {
                            edges.push(CodeEdge {
                                kind: CodeEdgeKind::Calls,
                                from_symbol: from_symbol.map(str::to_string),
                                to_symbol: name.to_string(),
                            });
                        }
                    }
                }
            }
        }
        Language::Python => {
            for line in source.lines() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("import ") {
                    let target = rest.split_whitespace().next().unwrap_or("").to_string();
                    if !target.is_empty() {
                        edges.push(CodeEdge {
                            kind: CodeEdgeKind::Imports,
                            from_symbol: from_symbol.map(str::to_string),
                            to_symbol: target,
                        });
                    }
                }
                if let Some(rest) = t.strip_prefix("from ") {
                    let target = rest.split_whitespace().next().unwrap_or("").to_string();
                    if !target.is_empty() {
                        edges.push(CodeEdge {
                            kind: CodeEdgeKind::Imports,
                            from_symbol: from_symbol.map(str::to_string),
                            to_symbol: target,
                        });
                    }
                }
            }
            if let Some(sym) = from_symbol {
                if let Some(target) = sym.strip_prefix("test_") {
                    edges.push(CodeEdge {
                        kind: CodeEdgeKind::TestedBy,
                        from_symbol: Some(sym.to_string()),
                        to_symbol: target.to_string(),
                    });
                }
            }
        }
        Language::TypeScript | Language::JavaScript => {
            for line in source.lines() {
                let t = line.trim();
                if t.starts_with("import ") {
                    if let Some(from) = t.split(" from ").nth(1) {
                        let target = from
                            .trim()
                            .trim_matches(';')
                            .trim_matches('"')
                            .trim_matches('\'')
                            .to_string();
                        if !target.is_empty() {
                            edges.push(CodeEdge {
                                kind: CodeEdgeKind::Imports,
                                from_symbol: from_symbol.map(str::to_string),
                                to_symbol: target,
                            });
                        }
                    }
                }
            }
        }
        Language::Go => {
            let mut in_import = false;
            for line in source.lines() {
                let t = line.trim();
                if t.starts_with("import (") {
                    in_import = true;
                    continue;
                }
                if in_import {
                    if t.starts_with(')') {
                        in_import = false;
                        continue;
                    }
                    let target = t.trim_matches('"').to_string();
                    if !target.is_empty() && !target.starts_with("//") {
                        edges.push(CodeEdge {
                            kind: CodeEdgeKind::Imports,
                            from_symbol: from_symbol.map(str::to_string),
                            to_symbol: target,
                        });
                    }
                } else if let Some(rest) = t.strip_prefix("import ") {
                    let target = rest.trim().trim_matches('"').to_string();
                    if !target.is_empty() {
                        edges.push(CodeEdge {
                            kind: CodeEdgeKind::Imports,
                            from_symbol: from_symbol.map(str::to_string),
                            to_symbol: target,
                        });
                    }
                }
            }
            if let Some(sym) = from_symbol {
                if let Some(target) = sym.strip_prefix("Test") {
                    edges.push(CodeEdge {
                        kind: CodeEdgeKind::TestedBy,
                        from_symbol: Some(sym.to_string()),
                        to_symbol: target.to_string(),
                    });
                }
            }
        }
        Language::Other => {}
    }
    edges
}

fn line_byte_range(source: &str, start_line: usize, end_line: usize) -> (usize, usize) {
    let start_line = start_line.max(1);
    let end_line = end_line.max(start_line);
    let mut start_byte = 0usize;
    let mut end_byte = source.len();
    let mut pos = 0usize;
    for (idx, part) in source.split_inclusive('\n').enumerate() {
        let line = idx + 1;
        if line == start_line {
            start_byte = pos;
        }
        if line == end_line {
            end_byte = pos + part.len();
            break;
        }
        pos += part.len();
    }
    (start_byte, end_byte)
}

fn make_chunk(
    source: &str,
    kind: SymbolKind,
    name: Option<String>,
    text: String,
    start_line: usize,
    end_line: usize,
) -> CodeChunk {
    let (start_byte, end_byte) = line_byte_range(source, start_line, end_line);
    CodeChunk {
        kind,
        name,
        text,
        start_line,
        end_line,
        start_byte,
        end_byte,
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
        if let Some(attr_start) = rust_attribute_block_before(lines, s) {
            s = attr_start;
            continue;
        }

        let prev = lines[s - 1].trim_start();
        if is_rust_comment_decoration(prev) {
            s -= 1;
        } else {
            break;
        }
    }
    s
}

fn is_rust_comment_decoration(trimmed: &str) -> bool {
    trimmed.starts_with("///") || trimmed.starts_with("//!") || trimmed.starts_with("//")
}

fn rust_attribute_block_before(lines: &[&str], end: usize) -> Option<usize> {
    let prev = lines.get(end.checked_sub(1)?)?.trim_start();
    if is_rust_attribute_start(prev) {
        return Some(end - 1);
    }

    if !prev.starts_with(']') && !prev.starts_with(')') {
        return None;
    }

    let mut i = end - 1;
    while i > 0 {
        i -= 1;
        let trimmed = lines[i].trim_start();
        if trimmed.is_empty()
            || is_rust_comment_decoration(trimmed)
            || rust_symbol_start(trimmed).is_some()
        {
            return None;
        }
        if is_rust_attribute_start(trimmed) {
            return rust_attribute_block_reaches(lines, i, end).then_some(i);
        }
    }

    None
}

fn is_rust_attribute_start(trimmed: &str) -> bool {
    trimmed.starts_with("#[") || trimmed.starts_with("#![")
}

fn rust_attribute_block_reaches(lines: &[&str], start: usize, end: usize) -> bool {
    let mut depth = 0i32;
    let mut saw_attribute_start = false;
    let mut state = RustScanState::default();

    for line in &lines[start..end] {
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if let Some(hashes) = state.raw_string_hashes {
                if let Some(end) = rust_raw_string_closing_end(&chars, i, hashes) {
                    state.raw_string_hashes = None;
                    i = end + 1;
                } else {
                    break;
                }
                continue;
            }

            if state.block_comment_depth > 0 {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    state.block_comment_depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    state.block_comment_depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            match chars[i] {
                '/' if chars.get(i + 1) == Some(&'/') => break,
                '/' if chars.get(i + 1) == Some(&'*') => {
                    state.block_comment_depth = 1;
                    i += 1;
                }
                'r' => {
                    if let Some((hashes, content_start)) = rust_raw_string_start(&chars, i) {
                        if let Some(end) =
                            rust_raw_string_closing_end(&chars, content_start, hashes)
                        {
                            i = end;
                        } else {
                            state.raw_string_hashes = Some(hashes);
                            break;
                        }
                    }
                }
                'b' if chars.get(i + 1) == Some(&'r') => {
                    if let Some((hashes, content_start)) = rust_raw_string_start(&chars, i + 1) {
                        if let Some(end) =
                            rust_raw_string_closing_end(&chars, content_start, hashes)
                        {
                            i = end;
                        } else {
                            state.raw_string_hashes = Some(hashes);
                            break;
                        }
                    }
                }
                '"' => i = skip_rust_string(&chars, i),
                '\'' => {
                    if let Some(end) = rust_char_literal_end(&chars, i) {
                        i = end;
                    }
                }
                '[' => {
                    saw_attribute_start = true;
                    depth += 1;
                }
                ']' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
    }

    saw_attribute_start && depth == 0
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
    if rest.starts_with("fn ")
        || rest.starts_with("async ")
        || rest.starts_with("unsafe ")
        || rest.starts_with("extern ")
    {
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
        match rust_generic_params_end(stripped) {
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
    let target = strip_rust_reference_target_prefix(target);
    let name: String = target
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn strip_rust_reference_target_prefix(mut target: &str) -> &str {
    target = target.trim_start();
    if let Some(rest) = target.strip_prefix('&') {
        target = rest.trim_start();
        if let Some(rest) = strip_rust_lifetime_prefix(target) {
            target = rest.trim_start();
        }
        for prefix in ["mut", "const"] {
            if let Some(rest) = strip_rust_word_prefix(target, prefix) {
                target = rest;
            }
        }
    }
    target
}

fn strip_rust_lifetime_prefix(target: &str) -> Option<&str> {
    let rest = target.strip_prefix('\'')?;
    let lifetime_len = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .map(char::len_utf8)
        .sum::<usize>();
    (lifetime_len > 0).then_some(&rest[lifetime_len..])
}

fn rust_generic_params_end(s: &str) -> Option<usize> {
    let mut depth = 1usize;
    for (idx, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

/// Find the line index where a Rust item starting at `start` ends.
fn rust_block_end(lines: &[&str], start: usize) -> usize {
    let mut depth: i32 = 0;
    let mut opened = false;
    let mut state = RustScanState::default();
    for (offset, line) in lines[start..].iter().enumerate() {
        let scan = rust_brace_scan_with_state(line, &mut state);
        if scan.saw_open {
            opened = true;
        }
        depth += scan.delta;
        if opened && depth <= 0 {
            return start + offset;
        }
        // One-liner item (no body): terminated by ';' before any '{'.
        if !opened && scan.saw_semicolon {
            return start + offset;
        }
    }
    lines.len() - 1
}

#[derive(Debug, Clone, Copy)]
struct RustBraceScan {
    delta: i32,
    saw_open: bool,
    saw_semicolon: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct RustScanState {
    block_comment_depth: u32,
    raw_string_hashes: Option<usize>,
    in_string: bool,
}

fn rust_brace_scan_with_state(line: &str, state: &mut RustScanState) -> RustBraceScan {
    let chars: Vec<char> = line.chars().collect();
    let mut delta = 0;
    let mut saw_open = false;
    let mut saw_semicolon = false;
    let mut i = 0;

    while i < chars.len() {
        if let Some(hashes) = state.raw_string_hashes {
            if let Some(end) = rust_raw_string_closing_end(&chars, i, hashes) {
                state.raw_string_hashes = None;
                i = end + 1;
            } else {
                break;
            }
            continue;
        }

        if state.in_string {
            if let Some(end) = rust_string_closing_end(&chars, i) {
                state.in_string = false;
                i = end + 1;
            } else {
                break;
            }
            continue;
        }

        if state.block_comment_depth > 0 {
            if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                state.block_comment_depth += 1;
                i += 2;
            } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                state.block_comment_depth -= 1;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        match chars[i] {
            '/' if chars.get(i + 1) == Some(&'/') => break,
            '/' if chars.get(i + 1) == Some(&'*') => {
                state.block_comment_depth = 1;
                i += 1;
            }
            'r' => {
                if let Some((hashes, content_start)) = rust_raw_string_start(&chars, i) {
                    if let Some(end) = rust_raw_string_closing_end(&chars, content_start, hashes) {
                        i = end;
                    } else {
                        state.raw_string_hashes = Some(hashes);
                        break;
                    }
                }
            }
            'b' if chars.get(i + 1) == Some(&'r') => {
                if let Some((hashes, content_start)) = rust_raw_string_start(&chars, i + 1) {
                    if let Some(end) = rust_raw_string_closing_end(&chars, content_start, hashes) {
                        i = end;
                    } else {
                        state.raw_string_hashes = Some(hashes);
                        break;
                    }
                }
            }
            '"' => {
                if let Some(end) = rust_string_closing_end(&chars, i + 1) {
                    i = end;
                } else {
                    state.in_string = true;
                    break;
                }
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
            ';' => saw_semicolon = true,
            _ => {}
        }
        i += 1;
    }

    RustBraceScan {
        delta,
        saw_open,
        saw_semicolon,
    }
}

fn rust_raw_string_start(chars: &[char], start: usize) -> Option<(usize, usize)> {
    if chars.get(start) != Some(&'r') {
        return None;
    }

    let mut i = start + 1;
    let mut hashes = 0usize;

    while chars.get(i) == Some(&'#') {
        hashes += 1;
        i += 1;
    }

    if chars.get(i) != Some(&'"') {
        return None;
    }

    Some((hashes, i + 1))
}

fn rust_raw_string_closing_end(chars: &[char], start: usize, hashes: usize) -> Option<usize> {
    let mut i = start;
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
    None
}

fn rust_string_closing_end(chars: &[char], start: usize) -> Option<usize> {
    let mut i = start;
    let mut escaped = false;
    while i < chars.len() {
        let ch = chars[i];
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn skip_rust_string(chars: &[char], start: usize) -> usize {
    rust_string_closing_end(chars, start + 1).unwrap_or_else(|| chars.len().saturating_sub(1))
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
    let mut state = RustScanState::default();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        if depth == 0 {
            if let Some(kind) = rust_symbol_start(trimmed) {
                let name = rust_symbol_name(trimmed, kind);
                let end = rust_block_end(&lines, i);
                let chunk_start = extend_start_with_decorations(&lines, i);
                chunks.push(make_chunk(
                    source,
                    kind,
                    name,
                    lines[chunk_start..=end].join("\n"),
                    chunk_start + 1,
                    end + 1,
                ));
                i = end + 1;
                continue;
            }
        }
        depth += rust_brace_scan_with_state(lines[i], &mut state).delta;
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
            let chunk_start = extend_start_with_python_decorators(&lines, i);
            chunks.push(make_chunk(
                source,
                kind,
                name,
                lines[chunk_start..=end].join("\n"),
                chunk_start + 1,
                end + 1,
            ));
            i = end + 1;
            continue;
        }
        i += 1;
    }
    chunks
}

/// TypeScript / JavaScript: top-level function / class / export function.
fn chunk_js_family(source: &str) -> Vec<CodeChunk> {
    let lines: Vec<&str> = source.lines().collect();
    let mut chunks = Vec::new();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        if depth == 0 {
            if let Some((kind, name)) = js_symbol_start(trimmed) {
                let end = brace_block_end(&lines, i);
                let chunk_start = i;
                chunks.push(make_chunk(
                    source,
                    kind,
                    name,
                    lines[chunk_start..=end].join("\n"),
                    chunk_start + 1,
                    end + 1,
                ));
                i = end + 1;
                continue;
            }
        }
        depth += brace_delta(lines[i]);
        if depth < 0 {
            depth = 0;
        }
        i += 1;
    }
    chunks
}

fn js_symbol_start(trimmed: &str) -> Option<(SymbolKind, Option<String>)> {
    let t = trimmed.trim_start_matches("export ").trim_start_matches("default ");
    if t.starts_with("function ") || t.starts_with("async function ") {
        let rest = t
            .trim_start_matches("async ")
            .trim_start_matches("function ")
            .trim_start();
        let name = rest
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        return Some((SymbolKind::Function, name));
    }
    if t.starts_with("class ") {
        let rest = t.trim_start_matches("class ").trim_start();
        let name = rest
            .split(|c: char| c == '{' || c.is_whitespace())
            .next()
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        return Some((SymbolKind::Class, name));
    }
    // const foo = (...) => { or const foo = function
    if (t.starts_with("const ") || t.starts_with("let ") || t.starts_with("var "))
        && (t.contains("=>") || t.contains("function"))
    {
        let after = t
            .split_whitespace()
            .nth(1)
            .unwrap_or("")
            .trim_end_matches('=')
            .trim()
            .to_string();
        if !after.is_empty() {
            return Some((SymbolKind::Function, Some(after)));
        }
    }
    None
}

fn chunk_go(source: &str) -> Vec<CodeChunk> {
    let lines: Vec<&str> = source.lines().collect();
    let mut chunks = Vec::new();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        if depth == 0 && trimmed.starts_with("func ") {
            let name = go_func_name(trimmed);
            let end = brace_block_end(&lines, i);
            chunks.push(make_chunk(
                source,
                SymbolKind::Function,
                name,
                lines[i..=end].join("\n"),
                i + 1,
                end + 1,
            ));
            i = end + 1;
            continue;
        }
        depth += brace_delta(lines[i]);
        if depth < 0 {
            depth = 0;
        }
        i += 1;
    }
    chunks
}

fn go_func_name(trimmed: &str) -> Option<String> {
    // func Name( or func (r *T) Name(
    let rest = trimmed.trim_start_matches("func ").trim_start();
    if rest.starts_with('(') {
        // method: find ) then name
        let after = rest.find(')').map(|i| rest[i + 1..].trim_start())?;
        let name = after
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()?
            .to_string();
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    } else {
        let name = rest
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()?
            .to_string();
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    }
}

fn brace_delta(line: &str) -> i32 {
    let mut delta = 0i32;
    let mut in_str = false;
    let mut quote = '\0';
    let mut escaped = false;
    for c in line.chars() {
        if in_str {
            if escaped {
                escaped = false;
                continue;
            }
            if c == '\\' {
                escaped = true;
                continue;
            }
            if c == quote {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' | '\'' | '`' => {
                in_str = true;
                quote = c;
            }
            '{' => delta += 1,
            '}' => delta -= 1,
            _ => {}
        }
    }
    delta
}

fn brace_block_end(lines: &[&str], start: usize) -> usize {
    let mut depth = 0i32;
    let mut seen_open = false;
    for (idx, line) in lines.iter().enumerate().skip(start) {
        let d = brace_delta(line);
        if d > 0 {
            seen_open = true;
        }
        depth += d;
        if seen_open && depth <= 0 {
            return idx;
        }
        // single-line bodies without braces: stop at same line
        if !seen_open && idx > start && !line.trim().is_empty() && !line.trim().ends_with('{') {
            // keep scanning until we see a brace or blank? Prefer end at start for arrow one-liners
        }
    }
    if !seen_open {
        return start;
    }
    lines.len().saturating_sub(1)
}

fn extend_start_with_python_decorators(lines: &[&str], start: usize) -> usize {
    let mut s = start;
    while let Some(decorator_start) = python_decorator_block_before(lines, s) {
        s = decorator_start;
    }
    s
}

fn python_decorator_block_before(lines: &[&str], end: usize) -> Option<usize> {
    let mut i = end;
    while i > 0 {
        i -= 1;
        let trimmed = lines[i].trim_start();
        if trimmed.is_empty() || is_python_top_level_symbol(lines[i]) {
            return None;
        }
        if trimmed.starts_with('@') {
            return python_decorator_block_reaches(lines, i, end).then_some(i);
        }
    }
    None
}

fn python_decorator_block_reaches(lines: &[&str], start: usize, end: usize) -> bool {
    let mut depth = 0i32;
    let mut state = PythonScanState::default();
    for (offset, line) in lines[start..end].iter().enumerate() {
        if offset > 0 && is_python_top_level_symbol(line) {
            return false;
        }
        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            return false;
        }
        let chars: Vec<char> = trimmed.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if let Some(quote) = state.triple_quote {
                if let Some(end) = python_triple_string_closing_end(&chars, i, quote) {
                    state.triple_quote = None;
                    i = end + 1;
                } else {
                    break;
                }
                continue;
            }

            match chars[i] {
                '"' | '\'' => {
                    let Some(end) = skip_python_string_or_update_state(&chars, i, &mut state)
                    else {
                        break;
                    };
                    i = end;
                }
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth = (depth - 1).max(0),
                '#' if depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
    }
    depth == 0
}

fn is_python_top_level_symbol(line: &str) -> bool {
    leading_indent(line) == 0
        && (line.starts_with("def ")
            || line.starts_with("async def ")
            || line.starts_with("class "))
}

fn python_block_end(lines: &[&str], start: usize) -> usize {
    let mut end = start;
    let mut header_depth = 0i32;
    let mut header_state = PythonScanState::default();
    let mut header_complete =
        python_header_complete(lines[start], &mut header_depth, &mut header_state);
    let mut j = start + 1;

    while j < lines.len() {
        let trimmed = lines[j].trim();
        if trimmed.is_empty() {
            j += 1;
            continue;
        }

        if !header_complete {
            end = j;
            header_complete = python_header_complete(trimmed, &mut header_depth, &mut header_state);
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

#[derive(Debug, Default, Clone, Copy)]
struct PythonScanState {
    triple_quote: Option<char>,
}

fn python_header_complete(line: &str, depth: &mut i32, state: &mut PythonScanState) -> bool {
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if let Some(quote) = state.triple_quote {
            if let Some(end) = python_triple_string_closing_end(&chars, i, quote) {
                state.triple_quote = None;
                i = end + 1;
            } else {
                break;
            }
            continue;
        }

        match chars[i] {
            '"' | '\'' => {
                let Some(end) = skip_python_string_or_update_state(&chars, i, state) else {
                    break;
                };
                i = end;
            }
            '(' | '[' | '{' => *depth += 1,
            ')' | ']' | '}' => *depth = (*depth - 1).max(0),
            ':' if *depth == 0 => return true,
            '#' if *depth == 0 => break,
            _ => {}
        }
        i += 1;
    }
    false
}

fn skip_python_string_or_update_state(
    chars: &[char],
    start: usize,
    state: &mut PythonScanState,
) -> Option<usize> {
    let quote = chars[start];
    if is_python_triple_string_start(chars, start, quote) {
        let content_start = start + 3;
        if let Some(end) = python_triple_string_closing_end(chars, content_start, quote) {
            Some(end)
        } else {
            state.triple_quote = Some(quote);
            None
        }
    } else {
        Some(skip_python_string(chars, start))
    }
}

fn is_python_triple_string_start(chars: &[char], start: usize, quote: char) -> bool {
    chars.get(start + 1) == Some(&quote) && chars.get(start + 2) == Some(&quote)
}

fn python_triple_string_closing_end(chars: &[char], start: usize, quote: char) -> Option<usize> {
    let mut i = start;
    while i < chars.len() {
        if chars[i] == quote && chars.get(i + 1) == Some(&quote) && chars.get(i + 2) == Some(&quote)
        {
            return Some(i + 2);
        }
        i += 1;
    }
    None
}

fn skip_python_string(chars: &[char], start: usize) -> usize {
    let quote = chars[start];
    let triple_quoted = is_python_triple_string_start(chars, start, quote);
    let mut i = if triple_quoted { start + 3 } else { start + 1 };
    let mut escaped = false;

    while i < chars.len() {
        let ch = chars[i];
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if triple_quoted {
            if ch == quote && chars.get(i + 1) == Some(&quote) && chars.get(i + 2) == Some(&quote) {
                return i + 2;
            }
        } else if ch == quote {
            return i;
        }
        i += 1;
    }

    chars.len().saturating_sub(1)
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
    fn test_rust_braces_inside_multiline_strings_do_not_break_scanning() {
        let src = "\
fn first() {
    let template = \"
raw text with } brace
\";
}

fn second() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert!(chunks[0].text.contains("raw text with } brace"));
        assert_eq!(chunks[0].end_line, 5);
        assert_eq!(chunks[1].start_line, 7);
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
    fn test_rust_braces_inside_nested_block_comments_do_not_break_scanning() {
        let src = "\
fn first() {
    /*
     * outer comment starts
     * /* inner comment */
     * still outer comment with }
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
        assert_eq!(chunks[0].end_line, 8);
        assert_eq!(chunks[1].start_line, 10);
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
    fn test_rust_braces_inside_multiline_raw_strings_do_not_break_scanning() {
        let src = "\
fn first() {
    let template = r#\"
raw text with } brace
\"#;
}

fn second() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["first", "second"]);
        assert!(chunks[0].text.contains("raw text with } brace"));
        assert!(chunks[0].text.contains("\"#;"));
        assert_eq!(chunks[0].end_line, 5);
        assert_eq!(chunks[1].start_line, 7);
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
    fn test_rust_semicolon_inside_multiline_const_string_does_not_end_chunk() {
        let src = "\
pub const QUERY: &str = \"
select *;
from contracts
\";

pub fn search() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();
        assert_eq!(names, vec!["QUERY", "search"]);
        assert_eq!(chunks[0].kind, SymbolKind::Const);
        assert!(chunks[0].text.contains("from contracts"));
        assert_eq!(chunks[0].end_line, 4);
        assert_eq!(chunks[1].start_line, 6);
    }

    #[test]
    fn test_rust_const_unsafe_extern_function_is_chunked() {
        let src = "\
pub const unsafe extern \"C\" fn stable_ffi() -> usize {
    42
}

pub fn next_symbol() {}";
        let chunks = chunk_code(src, Language::Rust);
        let names: Vec<&str> = chunks
            .iter()
            .filter_map(|chunk| chunk.name.as_deref())
            .collect();

        assert_eq!(names, vec!["stable_ffi", "next_symbol"]);
        assert_eq!(chunks[0].kind, SymbolKind::Function);
        assert!(chunks[0].text.contains("unsafe extern"));
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
    fn test_rust_multiline_attribute_attached() {
        let src = "\
#[cfg_attr(
    feature = \"serde\",
    derive(Serialize, Deserialize),
)]
pub struct ContractRecord {
    amount: u64,
}

pub fn next_symbol() {}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].kind, SymbolKind::Struct);
        assert_eq!(chunks[0].name.as_deref(), Some("ContractRecord"));
        assert!(chunks[0].text.contains("#[cfg_attr("));
        assert!(chunks[0].text.contains("derive(Serialize, Deserialize)"));
        assert_eq!(chunks[0].start_line, 1);
        assert_eq!(chunks[1].name.as_deref(), Some("next_symbol"));
        assert!(!chunks[1].text.contains("cfg_attr"));
    }

    #[test]
    fn test_rust_multiline_attribute_ignores_raw_string_brackets() {
        let src = r###"#[cfg_attr(
    feature = "docs",
    doc = r#"literal " ] bracket"#
)]
pub fn documented() {}

pub fn next_symbol() {}"###;
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].name.as_deref(), Some("documented"));
        assert!(chunks[0].text.contains("literal \" ] bracket"));
        assert_eq!(chunks[0].start_line, 1);
        assert_eq!(chunks[1].name.as_deref(), Some("next_symbol"));
        assert!(!chunks[1].text.contains("literal ] bracket"));
    }

    #[test]
    fn test_rust_attribute_does_not_leak_to_next_item() {
        let src = "\
#[derive(Debug)]
pub struct First;
pub struct Second;";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].text.contains("#[derive(Debug)]"));
        assert!(!chunks[1].text.contains("#[derive(Debug)]"));
        assert_eq!(chunks[1].name.as_deref(), Some("Second"));
    }

    #[test]
    fn test_rust_impl_trait_for_type_name() {
        let src = "impl fmt::Display for Point {\n    fn fmt(&self) {}\n}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks[0].kind, SymbolKind::Impl);
        assert_eq!(chunks[0].name.as_deref(), Some("Point"));
    }

    #[test]
    fn test_rust_impl_name_skips_nested_generic_params() {
        let src = "impl<T: Into<Vec<u8>>> Foo<T> {\n    fn build(value: T) {}\n}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks[0].kind, SymbolKind::Impl);
        assert_eq!(chunks[0].name.as_deref(), Some("Foo"));
    }

    #[test]
    fn test_rust_trait_impl_name_skips_nested_generic_params() {
        let src = "impl<T: Into<Vec<u8>>> From<T> for Foo<T> {\n    fn from(value: T) -> Self { Self }\n}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks[0].kind, SymbolKind::Impl);
        assert_eq!(chunks[0].name.as_deref(), Some("Foo"));
    }

    #[test]
    fn test_rust_trait_impl_name_skips_reference_target_prefix() {
        let src = "impl<'a> fmt::Display for &'a mut Foo {\n    fn fmt(&self) {}\n}";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks[0].kind, SymbolKind::Impl);
        assert_eq!(chunks[0].name.as_deref(), Some("Foo"));
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
    fn test_python_multiline_decorator_attached() {
        let src = "\
@router.get(
    \"/contracts/{customer}\",
    tags=[\"rag\", \"contracts\"],
)
@requires_acl(\"contracts:read\")
def contract_lookup(customer):
    return customer
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].name.as_deref(), Some("contract_lookup"));
        assert!(chunks[0].text.contains("@router.get("));
        assert!(chunks[0].text.contains("\"/contracts/{customer}\""));
        assert!(chunks[0].text.contains("@requires_acl"));
    }

    #[test]
    fn test_python_multiline_decorator_ignores_parens_inside_strings() {
        let src = "\
@router.get(
    path=\"/contracts/(legacy\",
)
def contract_lookup():
    return None
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].name.as_deref(), Some("contract_lookup"));
        assert!(chunks[0].text.contains("@router.get("));
        assert!(chunks[0].text.contains("/contracts/(legacy"));
    }

    #[test]
    fn test_python_multiline_decorator_ignores_parens_inside_triple_strings() {
        let src = r#"@router.get(
    path="""
/contracts/(legacy
""",
)
def contract_lookup():
    return None
"#;
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].name.as_deref(), Some("contract_lookup"));
        assert!(chunks[0].text.contains("@router.get("));
        assert!(chunks[0].text.contains("/contracts/(legacy"));
    }

    #[test]
    fn test_python_decorator_does_not_leak_to_next_function() {
        let src = "\
@decorator
def first():
    pass

def second():
    pass
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].text.contains("@decorator"));
        assert!(!chunks[1].text.contains("@decorator"));
        assert_eq!(chunks[1].name.as_deref(), Some("second"));
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
    fn test_python_multiline_signature_ignores_colon_inside_default_string() {
        let src = "\
def build_context(
    pattern=\"):\",
):
    return pattern

def next_symbol():
    return None
";
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].name.as_deref(), Some("build_context"));
        assert!(chunks[0].text.contains("pattern=\"):\","));
        assert!(chunks[0].text.contains("return pattern"));
        assert_eq!(chunks[0].end_line, 4);
        assert_eq!(chunks[1].name.as_deref(), Some("next_symbol"));
        assert_eq!(chunks[1].start_line, 6);
    }

    #[test]
    fn test_python_multiline_signature_ignores_colon_inside_triple_string() {
        let src = r#"def build_context(
    pattern="""
):
""",
):
    return pattern

def next_symbol():
    return None
"#;
        let chunks = chunk_code(src, Language::Python);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].name.as_deref(), Some("build_context"));
        assert!(chunks[0].text.contains("return pattern"));
        assert_eq!(chunks[0].end_line, 6);
        assert_eq!(chunks[1].name.as_deref(), Some("next_symbol"));
        assert_eq!(chunks[1].start_line, 8);
    }

    #[test]
    fn test_other_language_single_chunk() {
        let chunks = chunk_code("hello\nworld", Language::Other);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].kind, SymbolKind::Other);
        assert_eq!(chunks[0].end_line, 2);
        assert!(chunk_code("   ", Language::Other).is_empty());
        assert!(chunks[0].end_byte > chunks[0].start_byte);
    }

    #[test]
    fn test_typescript_function_and_class_chunks() {
        let src = r#"
import { foo } from "./foo";

export function greet(name: string) {
  return name;
}

export class Greeter {
  greet() {
    return "hi";
  }
}
"#;
        let chunks = chunk_code(src, Language::TypeScript);
        assert!(chunks.len() >= 2, "got {:?}", chunks.iter().map(|c| &c.name).collect::<Vec<_>>());
        assert!(chunks.iter().any(|c| c.name.as_deref() == Some("greet")));
        assert!(chunks.iter().any(|c| c.name.as_deref() == Some("Greeter")));
        let edges = extract_code_edges(src, Language::TypeScript, Some("greet"));
        assert!(edges.iter().any(|e| e.kind == CodeEdgeKind::Imports && e.to_symbol.contains("foo")));
    }

    #[test]
    fn test_go_func_chunks_and_test_edge() {
        let src = r#"
package demo

import "fmt"

func Add(a, b int) int {
  return a + b
}

func TestAdd(t *testing.T) {
  if Add(1, 2) != 3 {
    t.Fatal("nope")
  }
}
"#;
        let chunks = chunk_code(src, Language::Go);
        assert!(chunks.iter().any(|c| c.name.as_deref() == Some("Add")));
        assert!(chunks.iter().any(|c| c.name.as_deref() == Some("TestAdd")));
        let edges = extract_code_edges(src, Language::Go, Some("TestAdd"));
        assert!(edges.iter().any(|e| e.kind == CodeEdgeKind::TestedBy && e.to_symbol == "Add"));
        assert!(edges.iter().any(|e| e.kind == CodeEdgeKind::Imports));
    }

    #[test]
    fn test_byte_spans_cover_chunk_text() {
        let src = "fn alpha() {}\n\nfn beta() {}\n";
        let chunks = chunk_code(src, Language::Rust);
        assert_eq!(chunks.len(), 2);
        for c in &chunks {
            assert!(c.end_byte <= src.len());
            assert!(c.start_byte < c.end_byte);
            let slice = &src[c.start_byte..c.end_byte];
            assert!(slice.contains(c.name.as_deref().unwrap_or("")));
        }
    }
}
