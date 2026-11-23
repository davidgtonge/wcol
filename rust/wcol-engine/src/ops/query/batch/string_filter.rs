//! Byte-string predicates for batch filters (DuckDB `VARCHAR` without collation: raw bytes).

use std::sync::Arc;

use rustc_hash::FxHashSet;

use crate::ir::CmpOp;

#[derive(Clone, Debug)]
pub(crate) enum TextPred {
    Never,
    AnyValid,
    Eq(Arc<[u8]>),
    Ne(Arc<[u8]>),
    /// `true`: length 0; `false`: length > 0.
    Empty(bool),
    InSmall(Vec<Arc<[u8]>>),
    InSet(FxHashSet<Arc<[u8]>>),
    Like {
        matcher: LikeMatcher,
        negated: bool,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum LikeMatcher {
    /// Only `%`: every non-null value.
    Any,
    Exact(Arc<[u8]>),
    Prefix(Arc<[u8]>),
    Suffix(Arc<[u8]>),
    Contains(Arc<[u8]>),
    /// General pattern: the parts between `%`s, with the first / last anchored when the
    /// pattern does not start / end with `%`.
    Segments {
        segments: Vec<Segment>,
        anchored_start: bool,
        anchored_end: bool,
    },
}

/// Literal bytes and `_` (one UTF-8 character) between two `%`s.
#[derive(Clone, Debug)]
pub(crate) struct Segment {
    tokens: Vec<Token>,
    /// Characters the segment spans (literal characters plus one per `_`).
    chars: usize,
}

#[derive(Clone, Debug)]
enum Token {
    Literal(Arc<[u8]>),
    AnyChar,
}

pub(crate) fn compile_text_compare(op: CmpOp, text: &str) -> Option<TextPred> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Some(match op {
            CmpOp::Eq => TextPred::Empty(true),
            CmpOp::Ne => TextPred::Empty(false),
            _ => return None,
        });
    }
    Some(match op {
        CmpOp::Eq => TextPred::Eq(Arc::from(bytes)),
        CmpOp::Ne => TextPred::Ne(Arc::from(bytes)),
        _ => return None,
    })
}

pub(crate) fn compile_text_in(values: &[&str]) -> TextPred {
    let mut list: Vec<Arc<[u8]>> = values.iter().map(|s| Arc::from(s.as_bytes())).collect();
    list.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    list.dedup_by(|a, b| a == b);
    match list.len() {
        0 => TextPred::Never,
        1..=8 => TextPred::InSmall(list),
        _ => TextPred::InSet(list.into_iter().collect()),
    }
}

/// Compile a `LIKE` pattern. As in DuckDB: case-sensitive, no escape character, `%` is any
/// run of characters and `_` exactly one UTF-8 character.
pub(crate) fn compile_like(pattern: &str, negated: bool) -> TextPred {
    let anchored_start = !pattern.starts_with('%');
    let anchored_end = !pattern.ends_with('%');
    let segments: Vec<Segment> = pattern.split('%').map(segment_of).collect();
    let has_any_char = pattern.contains('_');
    let literal = |seg: &Segment| -> Arc<[u8]> {
        match seg.tokens.first() {
            Some(Token::Literal(bytes)) => Arc::clone(bytes),
            _ => Arc::from(&b""[..]),
        }
    };
    let non_empty: Vec<&Segment> = segments
        .iter()
        .filter(|seg| !seg.tokens.is_empty())
        .collect();
    let matcher = if non_empty.is_empty() {
        if segments.len() == 1 {
            LikeMatcher::Exact(Arc::from(&b""[..]))
        } else {
            LikeMatcher::Any
        }
    } else if has_any_char || non_empty.len() > 1 {
        LikeMatcher::Segments {
            segments,
            anchored_start,
            anchored_end,
        }
    } else {
        let only = literal(non_empty[0]);
        match (anchored_start, anchored_end) {
            (true, true) => LikeMatcher::Exact(only),
            (true, false) => LikeMatcher::Prefix(only),
            (false, true) => LikeMatcher::Suffix(only),
            (false, false) => LikeMatcher::Contains(only),
        }
    };
    TextPred::Like { matcher, negated }
}

fn segment_of(part: &str) -> Segment {
    let mut tokens = Vec::new();
    let mut literal = Vec::new();
    let mut chars = 0usize;
    for ch in part.chars() {
        chars += 1;
        if ch == '_' {
            if !literal.is_empty() {
                tokens.push(Token::Literal(Arc::from(std::mem::take(&mut literal))));
            }
            tokens.push(Token::AnyChar);
        } else {
            let mut buf = [0u8; 4];
            literal.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
        }
    }
    if !literal.is_empty() {
        tokens.push(Token::Literal(Arc::from(literal)));
    }
    Segment { tokens, chars }
}

#[inline]
pub(crate) fn test_bytes(pred: &TextPred, slice: &[u8]) -> bool {
    match pred {
        TextPred::Never => false,
        TextPred::AnyValid => true,
        TextPred::Eq(want) => slice == want.as_ref(),
        TextPred::Ne(want) => slice != want.as_ref(),
        TextPred::Empty(is_empty) => slice.is_empty() == *is_empty,
        TextPred::InSmall(values) => values.iter().any(|v| v.as_ref() == slice),
        TextPred::InSet(values) => values.contains(slice),
        TextPred::Like { matcher, negated } => {
            let hit = like_match(matcher, slice);
            if *negated {
                !hit
            } else {
                hit
            }
        }
    }
}

fn like_match(matcher: &LikeMatcher, haystack: &[u8]) -> bool {
    match matcher {
        LikeMatcher::Any => true,
        LikeMatcher::Exact(needle) => haystack == needle.as_ref(),
        LikeMatcher::Prefix(needle) => haystack.starts_with(needle.as_ref()),
        LikeMatcher::Suffix(needle) => haystack.ends_with(needle.as_ref()),
        LikeMatcher::Contains(needle) => find_subslice(haystack, needle.as_ref()).is_some(),
        LikeMatcher::Segments {
            segments,
            anchored_start,
            anchored_end,
        } => match_segments(segments, *anchored_start, *anchored_end, haystack),
    }
}

/// Segments in order: the first at 0 when anchored, each middle one at its leftmost match,
/// the last ending at the end when anchored. A segment spans a fixed number of characters, so
/// the leftmost match also ends earliest and no backtracking is needed: O(len * pattern).
fn match_segments(
    segments: &[Segment],
    anchored_start: bool,
    anchored_end: bool,
    text: &[u8],
) -> bool {
    let last = segments.len() - 1;
    if last == 0 {
        // No `%`: the whole value must match the one segment.
        return segment_at(&segments[0], text, 0) == Some(text.len());
    }
    let mut pos = 0usize;
    for (index, segment) in segments.iter().enumerate() {
        if segment.tokens.is_empty() {
            continue;
        }
        if index == 0 && anchored_start {
            match segment_at(segment, text, 0) {
                Some(end) => pos = end,
                None => return false,
            }
        } else if index == last && anchored_end {
            let Some(start) = back_chars(text, segment.chars) else {
                return false;
            };
            return start >= pos && segment_at(segment, text, start) == Some(text.len());
        } else {
            match find_segment(segment, text, pos) {
                Some(end) => pos = end,
                None => return false,
            }
        }
    }
    true
}

/// End of `segment` matched at byte `at`, if it matches there.
fn segment_at(segment: &Segment, text: &[u8], mut at: usize) -> Option<usize> {
    for token in &segment.tokens {
        match token {
            Token::Literal(bytes) => {
                if !text.get(at..)?.starts_with(bytes) {
                    return None;
                }
                at += bytes.len();
            }
            Token::AnyChar => {
                if at >= text.len() {
                    return None;
                }
                at += char_len(text, at);
            }
        }
    }
    Some(at)
}

/// End of the leftmost match of `segment` starting at or after `from`.
fn find_segment(segment: &Segment, text: &[u8], from: usize) -> Option<usize> {
    let mut start = from;
    while start <= text.len() {
        let candidate = match segment.tokens.first() {
            // Jump to the next place the leading literal occurs.
            Some(Token::Literal(lead)) => start + find_subslice(text.get(start..)?, lead)?,
            _ => start,
        };
        if let Some(end) = segment_at(segment, text, candidate) {
            return Some(end);
        }
        if candidate >= text.len() {
            return None;
        }
        start = candidate + char_len(text, candidate);
    }
    None
}

/// Byte offset `chars` UTF-8 characters before the end, if the text has that many.
fn back_chars(text: &[u8], chars: usize) -> Option<usize> {
    let mut at = text.len();
    for _ in 0..chars {
        if at == 0 {
            return None;
        }
        at -= 1;
        while at > 0 && text[at] & 0xc0 == 0x80 {
            at -= 1;
        }
    }
    Some(at)
}

/// Length of the UTF-8 character starting at `at` (1 for a stray byte, never past the end).
#[inline]
fn char_len(text: &[u8], at: usize) -> usize {
    let width = match text[at] {
        b if b < 0x80 => 1,
        b if b >> 5 == 0b110 => 2,
        b if b >> 4 == 0b1110 => 3,
        b if b >> 3 == 0b11110 => 4,
        _ => 1,
    };
    width.min(text.len() - at)
}

/// First index of `needle` in `haystack`. Checks the needle's first and last byte for 16
/// positions at a time (vectorises on wasm `simd128` and native) and compares only candidates.
pub(crate) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let n = needle.len();
    match n {
        0 => return Some(0),
        1 => return haystack.iter().position(|b| *b == needle[0]),
        _ => {}
    }
    if haystack.len() < n {
        return None;
    }
    let (first, last) = (needle[0], needle[n - 1]);
    let last_start = haystack.len() - n;
    let mut i = 0usize;
    while i + 16 <= last_start + 1 {
        let mut mask = 0u32;
        for j in 0..16 {
            mask |= u32::from(haystack[i + j] == first && haystack[i + j + n - 1] == last) << j;
        }
        while mask != 0 {
            let at = i + mask.trailing_zeros() as usize;
            if haystack[at + 1..at + n - 1] == needle[1..n - 1] {
                return Some(at);
            }
            mask &= mask - 1;
        }
        i += 16;
    }
    while i <= last_start {
        if haystack[i] == first && haystack[i + n - 1] == last && &haystack[i..i + n] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

pub(crate) type ByteDictKey = (Arc<Vec<u32>>, Arc<Vec<u8>>);

pub(crate) fn dict_entry_slice<'a>(offsets: &'a [u32], bytes: &'a [u8], id: usize) -> &'a [u8] {
    let start = offsets[id] as usize;
    let end = offsets[id + 1] as usize;
    &bytes[start..end]
}

pub(crate) fn utf8_row_slice<'a>(offsets: &'a [u32], bytes: &'a [u8], row: usize) -> &'a [u8] {
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference LIKE over characters (exponential, fine for short inputs).
    fn oracle(pattern: &[char], text: &[char]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some(('%', rest)) => (0..=text.len()).any(|skip| oracle(rest, &text[skip..])),
            Some(('_', rest)) => !text.is_empty() && oracle(rest, &text[1..]),
            Some((ch, rest)) => text.first() == Some(ch) && oracle(rest, &text[1..]),
        }
    }

    fn like(pattern: &str, text: &str) -> bool {
        test_bytes(&compile_like(pattern, false), text.as_bytes())
    }

    #[test]
    fn empty_string_predicates() {
        assert!(test_bytes(&TextPred::Empty(true), b""));
        assert!(!test_bytes(&TextPred::Empty(true), b"x"));
        assert!(test_bytes(&TextPred::Empty(false), b"x"));
    }

    #[test]
    fn like_contains_and_not() {
        assert!(like("%google%", "xgoogley"));
        let not = compile_like("%.google.%", true);
        assert!(!test_bytes(&not, b"www.google.com"));
        assert!(test_bytes(&not, b"evil.com"));
    }

    #[test]
    fn single_wildcards_and_empty_patterns_do_not_panic() {
        for pattern in ["%", "%%", "_", "", "%_", "_%", "a%", "%a"] {
            for text in ["", "a", "ab", "е"] {
                let p: Vec<char> = pattern.chars().collect();
                let t: Vec<char> = text.chars().collect();
                assert_eq!(
                    like(pattern, text),
                    oracle(&p, &t),
                    "{pattern:?} on {text:?}"
                );
            }
        }
    }

    #[test]
    fn underscore_matches_one_utf8_character() {
        assert!(like("_да", "еда"));
        assert!(!like("__да", "еда"));
        assert!(like("_едь%", "ведь это"));
        assert!(like("%ь_э%", "ведь это"));
    }

    #[test]
    fn matches_reference_on_random_patterns() {
        let alphabet = ['a', 'b', 'е', 'ж'];
        let pattern_chars = ['a', 'b', 'е', '%', '_'];
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        for _ in 0..20_000 {
            let text: String = (0..next(9))
                .map(|_| alphabet[next(alphabet.len())])
                .collect();
            let pattern: String = (0..next(7))
                .map(|_| pattern_chars[next(pattern_chars.len())])
                .collect();
            let p: Vec<char> = pattern.chars().collect();
            let t: Vec<char> = text.chars().collect();
            assert_eq!(
                like(&pattern, &text),
                oracle(&p, &t),
                "{pattern:?} on {text:?}"
            );
        }
    }

    #[test]
    fn many_wildcards_stay_linear() {
        let text = "a".repeat(20_000);
        let start = std::time::Instant::now();
        assert!(!like("%a%a%a%a%a%a%a%a%q%", &text));
        assert!(start.elapsed() < std::time::Duration::from_millis(200));
    }

    #[test]
    fn find_subslice_agrees_with_naive_search() {
        let hay = b"abracadabra-google-abracadabra-goo-gle-googlegoogle";
        for needle in [
            &b"google"[..],
            b"ab",
            b"a",
            b"gle",
            b"zz",
            b"abracadabra-google-abracadabra-goo-gle-googlegooglex",
        ] {
            let naive = hay.windows(needle.len()).position(|w| w == needle);
            assert_eq!(
                find_subslice(hay, needle),
                naive,
                "{:?}",
                std::str::from_utf8(needle)
            );
        }
    }
}
