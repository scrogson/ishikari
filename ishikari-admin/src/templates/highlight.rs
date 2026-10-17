//! Syntax highlighting for the code blocks, done when rendering so it needs no script. Tokens
//! go in `ik-tok-*` spans: `key`, `str`, `num`, `lit` (true, false, null), `p` (punctuation),
//! `comment`, and `expr` for a `{{ ... }}` expression inside a string.

/// JSON as HTML. Text that isn't JSON comes back escaped and plain.
pub fn json(src: &str) -> String {
    if serde_json::from_str::<serde_json::Value>(src).is_err() {
        return escape(src);
    }
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len() * 2);
    let mut i = 0;
    // Outside its strings, valid JSON is ASCII, so each token starts and ends on a char boundary.
    while i < b.len() {
        let start = i;
        let class = match b[i] {
            b'"' => {
                i = start + quoted_end(&src[start..]).unwrap_or(src.len() - start);
                if src[i..].trim_start().starts_with(':') {
                    "key"
                } else {
                    "str"
                }
            }
            b'-' | b'0'..=b'9' => {
                while i < b.len() && matches!(b[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                {
                    i += 1;
                }
                "num"
            }
            b'a'..=b'z' => {
                while i < b.len() && b[i].is_ascii_alphabetic() {
                    i += 1;
                }
                "lit"
            }
            b'{' | b'}' | b'[' | b']' | b':' | b',' => {
                i += 1;
                "p"
            }
            _ => {
                while i < b.len() && b[i].is_ascii_whitespace() {
                    i += 1;
                }
                i = i.max(start + 1);
                out.push_str(&src[start..i]);
                continue;
            }
        };
        token(&mut out, class, &src[start..i]);
    }
    out
}

/// YAML as HTML, a line at a time: keys, scalars by kind, flow collections, comments, and the
/// lines of a block scalar (`|`, `>`) as a string. Lenient: what it doesn't recognize is a
/// string, and the text always comes back unchanged.
pub fn yaml(src: &str) -> String {
    let mut out = String::with_capacity(src.len() * 2);
    // While in a block scalar, the column its lines must be indented past.
    let mut block: Option<usize> = None;
    for (n, line) in src.split('\n').enumerate() {
        if n > 0 {
            out.push('\n');
        }
        let rest = line.trim_start_matches(' ');
        let indent = line.len() - rest.len();
        out.push_str(&line[..indent]);
        if let Some(col) = block {
            if rest.trim().is_empty() || indent > col {
                token(&mut out, "str", rest);
                continue;
            }
            block = None;
        }
        yaml_line(&mut out, rest, indent, &mut block);
    }
    out
}

fn yaml_line(out: &mut String, mut rest: &str, mut col: usize, block: &mut Option<usize>) {
    if rest.starts_with('#') {
        return token(out, "comment", rest);
    }
    if rest == "---" || rest == "..." || rest.starts_with("--- ") {
        token(out, "p", &rest[..3]);
        return yaml_value(out, &rest[3..], col, block);
    }
    // Sequence entries, perhaps several on a line (`- - a`).
    while rest == "-" || rest.starts_with("- ") {
        token(out, "p", "-");
        let item = rest[1..].trim_start_matches(' ');
        out.push_str(&rest[1..rest.len() - item.len()]);
        col += rest.len() - item.len();
        rest = item;
    }
    match key_end(rest) {
        Some(colon) => {
            token(out, "key", &rest[..colon]);
            token(out, "p", ":");
            yaml_value(out, &rest[colon + 1..], col, block);
        }
        // A block scalar as a sequence entry: its lines are indented to the entry's content.
        None => yaml_value(out, rest, col.saturating_sub(1), block),
    }
}

/// Where the `:` ending the mapping key at the start of `s` is, if there's one.
fn key_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let colon = match b.first()? {
        b'"' | b'\'' => {
            let end = quoted_end(s)?;
            let gap = s[end..].len() - s[end..].trim_start_matches(' ').len();
            Some(end + gap).filter(|&c| b.get(c) == Some(&b':'))?
        }
        b'[' | b'{' | b'|' | b'>' | b'#' | b'&' | b'*' | b'!' | b'%' | b'@' | b'`' => return None,
        _ => {
            let mut from = 0;
            loop {
                let c = from + s[from..].find(':')?;
                if s[..c].contains(" #") {
                    return None;
                }
                if c + 1 == s.len() || b[c + 1] == b' ' {
                    break c;
                }
                from = c + 1;
            }
        }
    };
    (colon + 1 == s.len() || b[colon + 1] == b' ').then_some(colon)
}

fn yaml_value(out: &mut String, v: &str, col: usize, block: &mut Option<usize>) {
    let t = v.trim_start_matches(' ');
    out.push_str(&v[..v.len() - t.len()]);
    let (t, comment) = split_comment(t);
    let body = t.trim_end_matches(' ');
    match body.as_bytes().first() {
        None => {}
        Some(b'|' | b'>') => {
            token(out, "p", body);
            *block = Some(col);
        }
        Some(b'"' | b'\'') => token(out, "str", body),
        Some(b'[' | b'{') => flow(out, body),
        // An anchor, alias or tag, then perhaps a value.
        Some(b'&' | b'*' | b'!') => {
            let end = body.find(' ').unwrap_or(body.len());
            token(out, "lit", &body[..end]);
            yaml_value(out, &body[end..], col, block);
        }
        _ => token(out, scalar(body), body),
    }
    out.push_str(&t[body.len()..]);
    token(out, "comment", comment);
}

/// A value and the comment after it: from a `#` at its start or after a space, outside quotes.
fn split_comment(t: &str) -> (&str, &str) {
    let from = match t.as_bytes().first() {
        Some(b'#') => return ("", t),
        Some(b'"' | b'\'') => quoted_end(t).unwrap_or(t.len()),
        _ => 0,
    };
    match t[from..].find(" #") {
        Some(i) => t.split_at(from + i + 1),
        None => (t, ""),
    }
}

/// A flow collection, `[a, b]` or `{k: v}`.
fn flow(out: &mut String, s: &str) {
    let b = s.as_bytes();
    let delim = |i: usize| {
        matches!(b[i], b'[' | b']' | b'{' | b'}' | b',')
            || (b[i] == b':' && (i + 1 == b.len() || b[i + 1] == b' '))
    };
    let mut i = 0;
    while i < b.len() {
        let start = i;
        match b[i] {
            b'[' | b']' | b'{' | b'}' | b',' | b':' => {
                i += 1;
                token(out, "p", &s[start..i]);
            }
            b' ' => {
                while i < b.len() && b[i] == b' ' {
                    i += 1;
                }
                out.push_str(&s[start..i]);
            }
            b'"' | b'\'' => {
                i = start + quoted_end(&s[start..]).unwrap_or(s.len() - start);
                let class = if s[i..].trim_start().starts_with(':') {
                    "key"
                } else {
                    "str"
                };
                token(out, class, &s[start..i]);
            }
            _ => {
                while i < b.len() && !delim(i) {
                    i += 1;
                }
                let word = s[start..i].trim_end_matches(' ');
                let class = if s[start + word.len()..].starts_with(':') {
                    "key"
                } else {
                    scalar(word)
                };
                token(out, class, word);
                out.push_str(&s[start + word.len()..i]);
            }
        }
    }
}

/// A plain scalar's kind.
fn scalar(s: &str) -> &'static str {
    match s {
        "null" | "Null" | "NULL" | "~" | "true" | "True" | "TRUE" | "false" | "False" | "FALSE" => {
            "lit"
        }
        ".inf" | "-.inf" | "+.inf" | ".nan" | ".NaN" => "num",
        _ if s.bytes().any(|c| c.is_ascii_digit())
            && (s.parse::<f64>().is_ok()
                || s.strip_prefix("0x")
                    .is_some_and(|h| i64::from_str_radix(h, 16).is_ok())) =>
        {
            "num"
        }
        _ => "str",
    }
}

/// The index just past the closing quote of the string `s` starts with.
fn quoted_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let q = *b.first()?;
    let mut i = 1;
    while i < b.len() {
        if q == b'"' && b[i] == b'\\' {
            i += 2;
        } else if b[i] == q && q == b'\'' && b.get(i + 1) == Some(&b'\'') {
            i += 2; // '' is an escaped quote
        } else if b[i] == q {
            return Some(i + 1);
        } else {
            i += 1;
        }
    }
    None
}

/// Append `text`, escaped, in a span of `class`; a string's `{{ ... }}` expressions get spans
/// of their own.
fn token(out: &mut String, class: &str, text: &str) {
    if text.is_empty() {
        return;
    }
    out.push_str("<span class=\"ik-tok-");
    out.push_str(class);
    out.push_str("\">");
    let mut rest = text;
    if class == "str" {
        while let Some(open) = rest.find("{{") {
            let Some(close) = rest[open..].find("}}").map(|i| open + i + 2) else {
                break;
            };
            out.push_str(&escape(&rest[..open]));
            out.push_str("<span class=\"ik-tok-expr\">");
            out.push_str(&escape(&rest[open..close]));
            out.push_str("</span>");
            rest = &rest[close..];
        }
    }
    out.push_str(&escape(rest));
    out.push_str("</span>");
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::{json, yaml};

    /// The text with the markup taken out and unescaped.
    fn text(html: &str) -> String {
        let mut out = String::new();
        let mut tag = false;
        for c in html.chars() {
            match c {
                '<' => tag = true,
                '>' if tag => tag = false,
                _ if !tag => out.push(c),
                _ => {}
            }
        }
        out.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
    }

    /// The tokens of a kind, in order.
    fn tokens(html: &str, class: &str) -> Vec<String> {
        let open = format!(r#"<span class="ik-tok-{class}">"#);
        html.split(&open)
            .skip(1)
            .map(|s| text(&s[..s.find("</span>").unwrap()]))
            .collect()
    }

    #[test]
    fn json_tokens_are_wrapped_by_kind() {
        let html = json(r#"{"a": "x", "n": -1.5e3, "ok": true, "none": null, "l": [1]}"#);
        assert_eq!(
            html,
            concat!(
                r#"<span class="ik-tok-p">{</span>"#,
                r#"<span class="ik-tok-key">"a"</span><span class="ik-tok-p">:</span> "#,
                r#"<span class="ik-tok-str">"x"</span><span class="ik-tok-p">,</span> "#,
                r#"<span class="ik-tok-key">"n"</span><span class="ik-tok-p">:</span> "#,
                r#"<span class="ik-tok-num">-1.5e3</span><span class="ik-tok-p">,</span> "#,
                r#"<span class="ik-tok-key">"ok"</span><span class="ik-tok-p">:</span> "#,
                r#"<span class="ik-tok-lit">true</span><span class="ik-tok-p">,</span> "#,
                r#"<span class="ik-tok-key">"none"</span><span class="ik-tok-p">:</span> "#,
                r#"<span class="ik-tok-lit">null</span><span class="ik-tok-p">,</span> "#,
                r#"<span class="ik-tok-key">"l"</span><span class="ik-tok-p">:</span> "#,
                r#"<span class="ik-tok-p">[</span><span class="ik-tok-num">1</span>"#,
                r#"<span class="ik-tok-p">]</span><span class="ik-tok-p">}</span>"#,
            )
        );
    }

    #[test]
    fn json_strings_are_escaped_whole() {
        let html = json(r#"{"k\"<": "a \"quoted\" <b>& é \\"}"#);
        assert_eq!(tokens(&html, "key"), [r#""k\"<""#]);
        assert_eq!(tokens(&html, "str"), [r#""a \"quoted\" <b>& é \\""#]);
        assert!(html.contains("&lt;b&gt;&amp;"));
    }

    #[test]
    fn json_text_is_unchanged() {
        let src = "{\n  \"name\": \"日本 <x>\",\n  \"list\": [\n    1,\n    {}\n  ],\n  \"empty\": \"\"\n}";
        assert_eq!(text(&json(src)), src);
    }

    #[test]
    fn what_isnt_json_is_only_escaped() {
        assert_eq!(json("not <json> & such"), "not &lt;json&gt; &amp; such");
        assert_eq!(json(""), "");
    }

    #[test]
    fn expressions_in_strings_are_marked() {
        let html = json(r#"{"to": "{{inputs.email}} and {{ a.b }}", "n": "{{"}"#);
        assert_eq!(tokens(&html, "expr"), ["{{inputs.email}}", "{{ a.b }}"]);
        let html = yaml("when: '{{check.outputs.ok}}'");
        assert_eq!(tokens(&html, "expr"), ["{{check.outputs.ok}}"]);
    }

    const DEFINITION: &str = "\
# A definition
name: send-notification
version: 1
ratio: -0.5
inputs:
  user_id:
    type: string
    default: null   # none
    required: true
nodes:
  fetch:
    depends_on: []
    inputs:
      url: 'https://example.com/a?b=1#frag'
      query: SELECT a, b FROM t WHERE c = $1
      headers: {Accept: application/json, \"X-N\": 2}
      params:
      - '{{inputs.user_id}}'
      - - nested
      - sent_at: \"{{now()}}\"
        quoted: \"it's # not a comment\"
    script: |
      line one: not a key
        indented # not a comment
    after: ~
outputs: {}
---
- |-
  item block
- &anchor value
- *anchor
";

    #[test]
    fn yaml_text_is_unchanged() {
        assert_eq!(text(&yaml(DEFINITION)), DEFINITION);
    }

    #[test]
    fn yaml_tokens_are_wrapped_by_kind() {
        let html = yaml(DEFINITION);
        assert_eq!(
            tokens(&html, "key"),
            [
                "name",
                "version",
                "ratio",
                "inputs",
                "user_id",
                "type",
                "default",
                "required",
                "nodes",
                "fetch",
                "depends_on",
                "inputs",
                "url",
                "query",
                "headers",
                "Accept",
                r#""X-N""#,
                "params",
                "sent_at",
                "quoted",
                "script",
                "after",
                "outputs",
            ]
        );
        assert_eq!(tokens(&html, "num"), ["1", "-0.5", "2"]);
        assert_eq!(
            tokens(&html, "lit"),
            ["null", "true", "~", "&anchor", "*anchor"]
        );
        assert_eq!(tokens(&html, "comment"), ["# A definition", "# none"]);
        let strings = tokens(&html, "str");
        for s in [
            "string",
            "'https://example.com/a?b=1#frag'",
            "SELECT a, b FROM t WHERE c = $1",
            "application/json",
            "nested",
            "\"it's # not a comment\"",
            "line one: not a key",
            "indented # not a comment",
            "item block",
            "value",
        ] {
            assert!(strings.iter().any(|t| t == s), "{s:?} in {strings:?}");
        }
    }

    #[test]
    fn odd_yaml_comes_back_unchanged() {
        for src in [
            "",
            "\n\n",
            "key: 'unterminated",
            "\"unterminated: x",
            "a: \"esc \\\" é\" # c\r\nb:\tc\r\n",
            "- [a, 'b, c', {d: [e]}, \"f\"g, :]\n- -",
            "名前: 値 # コメント\n  - ü: ö",
            "k: |\n\n  text\n\nnext: >-\n  folded\n---\n...",
            "::: :\n- :\n? complex\n: value",
        ] {
            assert_eq!(text(&yaml(src)), src, "{src:?}");
        }
    }
}
