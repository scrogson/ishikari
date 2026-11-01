//! How deeply a script nests, checked before swc parses it. swc's parser, and its passes over
//! what it parsed, recurse once a level on the stack of the thread they run on, and a script
//! nested deeply enough overflows it, which aborts the process. A script that may nest deeper
//! than [`MAX_DEPTH`] is refused instead.
//!
//! The depth is an upper bound of the parser's, counted on tokens. A bracket opens a level, in
//! which each token counts one (an operator, an arrow or a keyword nests what follows it) until a
//! place nothing before it stays open across: a `,`, a `;`, or a line break where a semicolon is
//! inserted. The head of a statement whose body isn't a block (`if (a)`, `else`, a label) counts
//! until its level closes, since an `else` can go on nesting past its body's end. A `<` opens a
//! level too, which may be a comparison's: a comma between type arguments ends only that.
//!
//! What can't be counted so is refused: a `/` after which it takes a parser to tell division from
//! a regular expression (after a `}`, a `>` or `of`), an escape in a name, and brackets that don't
//! match.

/// How deep a script may nest. Stripping a script takes at most a few KiB of stack a level.
pub(crate) const MAX_DEPTH: usize = 2048;

/// Whether `source` nests at most [`MAX_DEPTH`] deep, or why it can't be stripped.
pub(crate) fn check(source: &str) -> Result<(), String> {
    Scan::new(source).run().map_err(|(what, at)| {
        let before = &source.as_bytes()[..at];
        let line = before.iter().filter(|&&b| b == b'\n').count() + 1;
        let line_start = before
            .iter()
            .rposition(|&b| b == b'\n')
            .map_or(0, |i| i + 1);
        // Characters, not bytes: a UTF-8 continuation byte is 10xxxxxx.
        let column = before[line_start..]
            .iter()
            .filter(|&&b| b & 0xc0 != 0x80)
            .count()
            + 1;
        format!("{what} (script:{line}:{column})")
    })
}

type Refused = (String, usize);

#[derive(Clone, Copy, PartialEq)]
enum Open {
    Script,
    /// A `(`, which `control` is when it's the head of an `if`, `for`, `while` or `with`.
    Paren {
        control: bool,
    },
    Bracket,
    Brace,
    /// A template's `${`.
    Substitution,
    /// A `<`: type arguments or parameters, or a comparison.
    Angle,
}

struct Level {
    open: Open,
    /// The tokens counted since the last place nothing stays open across.
    tokens: usize,
    /// The heads of statements whose body isn't a block.
    held: usize,
}

/// What the last token was, which tells what a `/` after it is, and whether a line break after
/// it may end a statement.
#[derive(Clone, Copy, PartialEq)]
enum Last {
    /// An operand (a name, a literal, a `)` or `]`): a `/` divides, and a word after a line
    /// break begins a statement.
    Operand,
    /// A keyword that may be a name: a `/` divides, and a line break may go on with its
    /// statement.
    Name,
    /// A `}`, which may end a block or an operand: what a `/` after it is takes a parser.
    Brace,
    /// An operator, or a keyword before an expression: a `/` begins a regular expression.
    Operator,
    /// A `>`, which may close type arguments, or `of`, which may be a name.
    Unsure,
}

/// Keywords before which a line break may go on with a statement after an operand.
const CONTINUES: &[&str] = &[
    "as",
    "asserts",
    "catch",
    "else",
    "extends",
    "finally",
    "from",
    "implements",
    "in",
    "instanceof",
    "is",
    "keyof",
    "of",
    "satisfies",
];

/// Keywords after which an expression, so a regular expression, may begin.
const BEFORE_EXPRESSION: &[&str] = &[
    "await",
    "case",
    "default",
    "delete",
    "do",
    "else",
    "extends",
    "for",
    "if",
    "in",
    "instanceof",
    "new",
    "return",
    "switch",
    "throw",
    "typeof",
    "void",
    "while",
    "with",
    "yield",
];

/// Keywords, TypeScript's included, which may be names: none ends a statement at a line break.
const KEYWORDS: &[&str] = &[
    "abstract",
    "accessor",
    "as",
    "asserts",
    "async",
    "break",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "declare",
    "enum",
    "export",
    "finally",
    "from",
    "function",
    "get",
    "global",
    "implements",
    "import",
    "infer",
    "interface",
    "is",
    "keyof",
    "let",
    "module",
    "namespace",
    "out",
    "override",
    "package",
    "private",
    "protected",
    "public",
    "readonly",
    "satisfies",
    "set",
    "static",
    "try",
    "type",
    "unique",
    "using",
    "var",
];

struct Scan<'a> {
    source: &'a str,
    bytes: &'a [u8],
    at: usize,
    levels: Vec<Level>,
    /// Each level, and what it counts.
    depth: usize,
    last: Last,
    /// A line terminator came after the last token.
    line_break: bool,
    /// The last token was `.` or `?.`, so a word is a property's name.
    property: bool,
    /// The last token was `if`, `for`, `while` or `with`, so a `(` is its head.
    control: bool,
    /// The last token was `for`, for `for await (`.
    after_for: bool,
    /// The last token ended a statement's head, so the next begins its body.
    body: bool,
    /// The last token was a word beginning a body, which a `:` makes a label.
    label: bool,
}

impl<'a> Scan<'a> {
    fn new(source: &'a str) -> Self {
        Scan {
            source,
            bytes: source.as_bytes(),
            at: 0,
            levels: vec![Level {
                open: Open::Script,
                tokens: 0,
                held: 0,
            }],
            depth: 1,
            last: Last::Operator,
            line_break: false,
            property: false,
            control: false,
            after_for: false,
            body: false,
            label: false,
        }
    }

    fn run(mut self) -> Result<(), Refused> {
        if self.bytes.starts_with(b"#!") {
            while self.at < self.bytes.len() && !self.line_terminator(self.at) {
                self.at += 1;
            }
        }
        while let Some(c) = self.space()? {
            let start = self.at;
            let label = std::mem::take(&mut self.label);
            if std::mem::take(&mut self.body) {
                if c != '{' {
                    self.top().held += 1;
                    self.grow(1, start)?;
                }
                self.label = is_word_start(c);
            }
            let property = std::mem::take(&mut self.property);
            let control = std::mem::take(&mut self.control);
            let after_for = std::mem::take(&mut self.after_for);
            let line_break = std::mem::take(&mut self.line_break);
            let next = self.bytes.get(start + 1).copied();
            match c {
                '"' | '\'' => {
                    self.string(c as u8)?;
                    self.count(start)?;
                    self.last = Last::Operand;
                }
                '`' => {
                    self.at += 1;
                    self.count(start)?;
                    self.template(start)?;
                }
                '0'..='9' => {
                    self.number();
                    self.count(start)?;
                    self.last = Last::Operand;
                }
                '.' if next.is_some_and(|b| b.is_ascii_digit()) => {
                    self.number();
                    self.count(start)?;
                    self.last = Last::Operand;
                }
                '\\' => return Err(("the script has an escape in a name".into(), start)),
                c if is_word_start(c) => self.word(start, property, after_for, line_break)?,
                '(' => {
                    self.at += 1;
                    self.push(Open::Paren { control }, start)?;
                }
                '[' => {
                    self.at += 1;
                    self.push(Open::Bracket, start)?;
                }
                '{' => {
                    self.at += 1;
                    self.push(Open::Brace, start)?;
                }
                ')' | ']' | '}' => self.close(c, start)?,
                ';' => {
                    self.at += 1;
                    self.end_statement();
                    self.last = Last::Operator;
                }
                ',' => {
                    self.at += 1;
                    self.restart();
                    self.last = Last::Operator;
                }
                '/' => {
                    match self.last {
                        Last::Operand | Last::Name => self.operator(1, start)?,
                        Last::Operator => {
                            self.regex(start)?;
                            self.count(start)?;
                            self.last = Last::Operand;
                        }
                        Last::Brace | Last::Unsure => return Err((
                            "the script has a / that may divide or begin a regular expression: \
                             put the regular expression, or what it divides, in parentheses"
                                .into(),
                            start,
                        )),
                    }
                }
                '<' => {
                    let run = self.run_of(b'<');
                    if self.bytes.get(start + run) == Some(&b'=') {
                        self.operator(run + 1, start)?;
                    } else {
                        self.at += run;
                        for _ in 0..run {
                            self.push(Open::Angle, start)?;
                        }
                    }
                }
                '>' => {
                    let run = self.run_of(b'>');
                    if self.bytes.get(start + run) == Some(&b'=') {
                        self.operator(run + 1, start)?;
                    } else {
                        self.at += run;
                        for _ in 0..run {
                            if self.top_open() == Open::Angle {
                                self.pop();
                            }
                        }
                        self.count(start)?;
                        self.last = Last::Unsure;
                    }
                }
                '=' if next == Some(b'>') => self.operator(2, start)?,
                '.' if self.bytes[start..].starts_with(b"...") => self.operator(3, start)?,
                '.' => {
                    self.operator(1, start)?;
                    self.property = true;
                }
                '?' if next == Some(b'.')
                    && !self
                        .bytes
                        .get(start + 2)
                        .is_some_and(|b| b.is_ascii_digit()) =>
                {
                    self.operator(2, start)?;
                    self.property = true;
                }
                // A postfix `!` (TypeScript's non-null assertion), `++` or `--` ends an
                // operand; a prefix one, after a line break included, begins one.
                '!' | '+' | '-' => {
                    let postfix = self.last == Last::Operand && !line_break;
                    let len = match (c, next) {
                        ('!', Some(b'=')) => 0,
                        ('!', _) => 1,
                        (_, Some(b)) if b == c as u8 => 2,
                        _ => 0,
                    };
                    if len > 0 && postfix {
                        self.at += len;
                        self.count(start)?;
                        self.last = Last::Operand;
                    } else {
                        self.operator(len.max(1), start)?;
                    }
                }
                ':' => {
                    self.operator(1, start)?;
                    self.body = label;
                }
                _ => self.operator(c.len_utf8(), start)?,
            }
        }
        Ok(())
    }

    /// Skips whitespace and comments, noting line breaks, to the next token's first char.
    fn space(&mut self) -> Result<Option<char>, Refused> {
        loop {
            let Some(&b) = self.bytes.get(self.at) else {
                return Ok(None);
            };
            match b {
                b'\n' | b'\r' => {
                    self.line_break = true;
                    self.at += 1;
                }
                b' ' | b'\t' | 0x0b | 0x0c => self.at += 1,
                b'/' if self.bytes.get(self.at + 1) == Some(&b'/') => {
                    while self.at < self.bytes.len() && !self.line_terminator(self.at) {
                        self.at += 1;
                    }
                }
                b'/' if self.bytes.get(self.at + 1) == Some(&b'*') => {
                    let start = self.at;
                    self.at += 2;
                    loop {
                        if self.at >= self.bytes.len() {
                            return Err(("the script has an unterminated comment".into(), start));
                        }
                        if self.bytes[self.at..].starts_with(b"*/") {
                            self.at += 2;
                            break;
                        }
                        self.line_break |= self.line_terminator(self.at);
                        self.at += 1;
                    }
                }
                0x80.. => {
                    let c = self.source[self.at..].chars().next().unwrap();
                    // Rust's whitespace has JavaScript's in it; a char of it a name may have
                    // only splits a word in two, counting more.
                    if !c.is_whitespace() && c != '\u{feff}' {
                        return Ok(Some(c));
                    }
                    self.line_break |= self.line_terminator(self.at);
                    self.at += c.len_utf8();
                }
                _ => return Ok(Some(b as char)),
            }
        }
    }

    /// Whether a line terminator (LF, CR, U+2028 or U+2029) is at `at`.
    fn line_terminator(&self, at: usize) -> bool {
        matches!(
            self.bytes.get(at..),
            Some([b'\n' | b'\r', ..] | [0xe2, 0x80, 0xa8 | 0xa9, ..])
        )
    }

    fn word(
        &mut self,
        start: usize,
        property: bool,
        after_for: bool,
        line_break: bool,
    ) -> Result<(), Refused> {
        self.at = start + 1;
        while let Some(&b) = self.bytes.get(self.at) {
            match b {
                b'\\' => return Err(("the script has an escape in a name".into(), self.at)),
                0x80.. => {
                    let c = self.source[self.at..].chars().next().unwrap();
                    if c.is_whitespace() || c == '\u{feff}' {
                        break;
                    }
                    self.at += c.len_utf8();
                }
                b if is_word_char(b) => self.at += 1,
                _ => break,
            }
        }
        let source = self.source;
        let word = &source[start..self.at];
        let keyword = |list: &[&str]| !property && list.contains(&word);
        if line_break && matches!(self.last, Last::Operand | Last::Brace) && !keyword(CONTINUES) {
            self.end_statement();
        }
        self.count(start)?;
        self.last = if property {
            Last::Operand
        } else if word == "of" {
            Last::Unsure
        } else if keyword(BEFORE_EXPRESSION) {
            Last::Operator
        } else if keyword(KEYWORDS) {
            Last::Name
        } else {
            Last::Operand
        };
        if !property {
            match word {
                "if" | "while" | "with" => self.control = true,
                "for" => {
                    self.control = true;
                    self.after_for = true;
                }
                "await" => self.control = after_for,
                "else" | "do" => self.body = true,
                _ => {}
            }
        }
        Ok(())
    }

    fn string(&mut self, quote: u8) -> Result<(), Refused> {
        let start = self.at;
        self.at += 1;
        loop {
            match self.bytes.get(self.at) {
                None | Some(b'\n' | b'\r') => {
                    return Err(("the script has an unterminated string".into(), start))
                }
                Some(&b) if b == quote => {
                    self.at += 1;
                    return Ok(());
                }
                Some(b'\\') if self.bytes[self.at + 1..].starts_with(b"\r\n") => self.at += 3,
                Some(b'\\') => self.at += 2,
                _ => self.at += 1,
            }
        }
    }

    /// A template's text, from after its `` ` `` or a substitution's `}`, to its end or a `${`,
    /// which opens a level.
    fn template(&mut self, start: usize) -> Result<(), Refused> {
        loop {
            match self.bytes.get(self.at) {
                None => return Err(("the script has an unterminated template".into(), start)),
                Some(b'`') => {
                    self.at += 1;
                    self.last = Last::Operand;
                    return Ok(());
                }
                Some(b'\\') => self.at += 2,
                Some(b'$') if self.bytes.get(self.at + 1) == Some(&b'{') => {
                    let at = self.at;
                    self.at += 2;
                    return self.push(Open::Substitution, at);
                }
                _ => self.at += 1,
            }
        }
    }

    fn regex(&mut self, start: usize) -> Result<(), Refused> {
        let unterminated = || {
            Err((
                "the script has an unterminated regular expression".into(),
                start,
            ))
        };
        self.at += 1;
        let mut class = false;
        loop {
            if self.at >= self.bytes.len() || self.line_terminator(self.at) {
                return unterminated();
            }
            match self.bytes[self.at] {
                b'\\' => {
                    if self.at + 1 >= self.bytes.len() || self.line_terminator(self.at + 1) {
                        return unterminated();
                    }
                    self.at += 2;
                    continue;
                }
                b'[' => class = true,
                b']' => class = false,
                b'/' if !class => {
                    self.at += 1;
                    break;
                }
                _ => {}
            }
            self.at += 1;
        }
        while self.bytes.get(self.at).is_some_and(|&b| is_word_char(b)) {
            self.at += 1;
        }
        Ok(())
    }

    /// A number: decimal, with a fraction and exponent, or hex, octal or binary, with
    /// separators and a BigInt's `n`.
    fn number(&mut self) {
        let digits = |scan: &mut Self, digit: fn(&u8) -> bool| {
            while scan
                .bytes
                .get(scan.at)
                .is_some_and(|b| digit(b) || *b == b'_')
            {
                scan.at += 1;
            }
        };
        let radix = matches!(
            self.bytes.get(self.at + 1),
            Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')
        );
        if self.bytes[self.at] == b'0' && radix {
            self.at += 2;
            digits(self, u8::is_ascii_hexdigit);
        } else {
            digits(self, u8::is_ascii_digit);
            if self.bytes.get(self.at) == Some(&b'.') {
                self.at += 1;
                digits(self, u8::is_ascii_digit);
            }
            if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
                self.at += 1;
                if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                    self.at += 1;
                }
                digits(self, u8::is_ascii_digit);
            }
        }
        if self.bytes.get(self.at) == Some(&b'n') {
            self.at += 1;
        }
    }

    fn run_of(&self, b: u8) -> usize {
        self.bytes[self.at..]
            .iter()
            .take_while(|&&c| c == b)
            .count()
    }

    /// An operator of `len` bytes.
    fn operator(&mut self, len: usize, start: usize) -> Result<(), Refused> {
        self.at += len;
        self.last = Last::Operator;
        self.count(start)
    }

    fn top(&mut self) -> &mut Level {
        self.levels.last_mut().unwrap()
    }

    fn top_open(&self) -> Open {
        self.levels.last().unwrap().open
    }

    fn grow(&mut self, by: usize, at: usize) -> Result<(), Refused> {
        self.depth += by;
        match self.depth > MAX_DEPTH {
            true => Err((format!("the script nests more than {MAX_DEPTH} deep"), at)),
            false => Ok(()),
        }
    }

    /// A token counted on the level it's on.
    fn count(&mut self, at: usize) -> Result<(), Refused> {
        self.top().tokens += 1;
        self.grow(1, at)
    }

    /// A bracket, counted on the level it's on, opening one.
    fn push(&mut self, open: Open, at: usize) -> Result<(), Refused> {
        self.count(at)?;
        self.levels.push(Level {
            open,
            tokens: 0,
            held: 0,
        });
        self.last = Last::Operator;
        self.grow(1, at)
    }

    fn pop(&mut self) -> Level {
        let level = self.levels.pop().unwrap();
        self.depth -= 1 + level.tokens + level.held;
        level
    }

    /// Nothing counted on the top level since here stays open after it.
    fn restart(&mut self) {
        let tokens = std::mem::take(&mut self.top().tokens);
        self.depth -= tokens;
    }

    /// A statement's end, which ends the comparisons of its `<`s too.
    fn end_statement(&mut self) {
        while self.top_open() == Open::Angle {
            self.pop();
        }
        self.restart();
    }

    fn close(&mut self, c: char, start: usize) -> Result<(), Refused> {
        self.at += 1;
        while self.top_open() == Open::Angle {
            self.pop();
        }
        let open = self.top_open();
        let matches = match c {
            ')' => matches!(open, Open::Paren { .. }),
            ']' => open == Open::Bracket,
            _ => matches!(open, Open::Brace | Open::Substitution),
        };
        if !matches {
            return Err(("the script's brackets don't match".into(), start));
        }
        self.pop();
        match open {
            Open::Paren { control: true } => {
                self.last = Last::Operator;
                self.body = true;
            }
            Open::Paren { .. } | Open::Bracket => self.last = Last::Operand,
            Open::Substitution => self.template(start)?,
            _ => self.last = Last::Brace,
        }
        Ok(())
    }
}

fn is_word_start(c: char) -> bool {
    c.is_ascii_alphabetic() || matches!(c, '_' | '$' | '#') || !c.is_ascii()
}

fn is_word_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$')
}

#[cfg(test)]
mod tests {
    use super::{check, MAX_DEPTH};

    fn refused(source: &str) -> String {
        check(source).expect_err("it was let through")
    }

    #[test]
    fn scripts_as_they_are_written_pass() {
        let typescript = r#"
            import { thing } from "./x";
            type Pair<A, B extends Array<Map<string, A>>> = [A, B];
            interface Spec { name: string; inputs?: Record<string, unknown> }
            export const spec: Spec = { name: "x", inputs: { a: 1, b: [1, 2, { c: `t${1 + 2}t` }] } };
            const re = /["'(\[{]+\/[a-z]/gi, half = 1 / 2 / 3;
            label: for (const x of [1, 2]) { if (x > 1) continue label; else break }
            if (a) b(); else if (c) d(); else { e() }
            do x++; while (x < 10)
            const f = <T,>(x: T): T => x!, g = async (y: number) => await f(y) as number;
            export function run(input: { n: number }) {
              return input.n < 3 ? `${input.n}` : String(input.n >>> 1) + '"' + "'";
            }
        "#;
        check(typescript).unwrap();
    }

    #[test]
    fn long_scripts_without_semicolons_pass() {
        let statement =
            "const a = b.c(d)\nif (a) {\n  e(a, f => f + 1)\n}\nfoo.bar = baz ? 1 : 2\n";
        check(&statement.repeat(5000)).unwrap();
        check(&format!("[{}]", "1, ".repeat(100_000))).unwrap();
        check(&format!("({{ {} }})", "a: 1, ".repeat(100_000))).unwrap();
        check(&format!("if (a) {{}}{}", " else if (a) {}".repeat(100))).unwrap();
    }

    #[test]
    fn each_way_of_nesting_is_counted() {
        let deep = 100_000;
        for source in [
            format!("{}1{}", "(".repeat(deep), ")".repeat(deep)),
            format!("{}1{}", "[".repeat(deep), "]".repeat(deep)),
            format!("{}{}", "{".repeat(deep), "}".repeat(deep)),
            format!("{}1{}", "`${".repeat(deep), "}`".repeat(deep)),
            format!("type T = {}1{};", "A<".repeat(deep), ">".repeat(deep)),
            format!("x = {}1", "a => ".repeat(deep)),
            format!("x = {}1", "!".repeat(deep)),
            format!("x = {}1", "a ? 1 : ".repeat(deep)),
            format!("x = {}1", "1 + ".repeat(deep)),
            format!("x = a{}", ".b".repeat(deep)),
            format!("x = f{}", "(1)".repeat(deep)),
            format!("x = {}1", "typeof ".repeat(deep)),
            format!("{}x", "if (a) ".repeat(deep)),
            format!("{}x", "do ".repeat(deep)),
            format!("{}x", "a: ".repeat(deep)),
            format!("if (a) x{}", "\nelse if (a) x".repeat(deep)),
            // Each body's end resets what's counted, and its else goes on nesting.
            format!(
                "if (a) if (a) x;{}",
                " else if (a) if (a) do x; while (a)".repeat(deep)
            ),
            // A comma between type arguments ends only theirs.
            format!("type T = {}1", "A<B, () => () => () => ".repeat(deep)),
        ] {
            let error = refused(&source);
            assert!(error.starts_with(&format!("the script nests more than {MAX_DEPTH} deep")));
        }
    }

    #[test]
    fn what_cant_be_counted_is_refused() {
        // A } may end a block, before a regular expression, or an operand, before division.
        assert!(refused("if (a) {}\n/re/.test(s)").contains("may divide"));
        assert!(refused("x = a > /re/.source").contains("may divide"));
        assert!(refused("const n = 1; const nm = \\u006e;").contains("escape in a name"));
        assert!(refused("f(a]").contains("brackets don't match"));
        assert!(refused("f(a))").contains("brackets don't match"));
        assert!(refused("x = 'abc\ny'").contains("unterminated string"));
        assert!(refused("x = /abc\ny/").contains("unterminated regular expression"));
        assert!(refused("x = `abc").contains("unterminated template"));
        assert!(refused("/* abc").contains("unterminated comment"));
    }

    #[test]
    fn nothing_deep_hides_in_a_comment_a_string_or_a_regular_expression() {
        let deep = "(".repeat(10_000);
        // A line comment ends at U+2028.
        refused(&format!("// a\u{2028}{deep}"));
        refused(&format!("x = /\"/; {deep}\""));
        refused(&format!("x = '\\''; {deep}'"));
        refused(&format!("x = `${{'`'}}`; {deep}`"));
        // A method named `if` doesn't head a statement, so the / after it divides.
        refused(&format!("x = a.if(1) / {deep} / 1"));
        check(&format!("x = /{deep}/")).unwrap();
        check(&format!("x = '{deep}'")).unwrap();
        check(&format!("// {deep}")).unwrap();
    }

    #[test]
    fn where_it_is_refused_is_named() {
        let error = refused(&format!("const a = 1;\nconst b = {}1;", "(".repeat(5000)));
        assert!(error.contains("(script:2:"), "{error}");
    }
}
