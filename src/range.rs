use std::{
    fmt::Debug,
    ops::{Range, RangeFrom, RangeTo},
};

mod merge;

pub use merge::*;

/// A (half-open) range bounded inclusively below and exclusively above
/// (`start..end`).
///
/// If `start >= end`, the range is considered wrapping and is equivalent to
/// covering two ranges: `(..end)` and `(start..)`.
pub struct KeyRange<Idx> {
    pub start: Idx,
    pub end: Idx,
}

impl<Idx> KeyRange<Idx> {
    /// Creates a `KeyRange`.
    pub fn new(start: Idx, end: Idx) -> Self {
        Self { start, end }
    }
}

impl<Idx: PartialOrd<Idx> + Clone> KeyRange<Idx> {
    /// Returns `true` if the range is wrapping, which is equivalent to covering
    /// the following two ranges: `(..end)` and `(start..)`.
    pub fn is_wrapping(&self) -> bool {
        !(self.start < self.end)
    }

    /// Returns `true` if `item` is contained in the range.
    pub fn contains(&self, item: &Idx) -> bool {
        if self.is_wrapping() {
            self.range_from().contains(&item) || self.range_to().contains(&item)
        } else {
            self.range_from().contains(&item) && self.range_to().contains(&item)
        }
    }

    /// Returns `true` if the range overlaps with `other`.
    pub fn is_overlapping(&self, other: &Self) -> bool {
        self.contains(&other.start) || other.contains(&self.start)
    }

    /// Extends both `start` and `end` of the range to match `other`.
    pub fn extend(&mut self, other: &Self) {
        self.extend_start(other);
        self.extend_end(other);
    }

    /// Extends the range's `start` to match `other.start` if `other.start <
    /// self.start`.
    pub fn extend_start(&mut self, other: &Self) {
        if other.start < self.start {
            if self.is_wrapping() && other.start < self.end {
                self.start = self.end.clone();
            } else {
                self.start = other.start.clone();
            }
        }
    }

    /// Extends the range's `end` to match `other.end` if `other.end >
    /// self.end`.
    pub fn extend_end(&mut self, other: &Self) {
        if other.end > self.end {
            if self.is_wrapping() && other.end > self.start {
                self.end = self.start.clone();
            } else {
                self.end = other.end.clone();
            }
        }
    }

    fn range_from(&self) -> RangeFrom<&Idx> {
        &self.start..
    }

    fn range_to(&self) -> RangeTo<&Idx> {
        ..&self.end
    }
}

impl<Idx> From<Range<Idx>> for KeyRange<Idx> {
    fn from(value: Range<Idx>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl<Idx: Clone> Clone for KeyRange<Idx> {
    fn clone(&self) -> Self {
        Self {
            start: self.start.clone(),
            end: self.end.clone(),
        }
    }
}

impl<Idx: PartialEq> PartialEq for KeyRange<Idx> {
    fn eq(&self, other: &Self) -> bool {
        self.start.eq(&other.start) && self.end.eq(&other.end)
    }
}

impl<Idx: Debug> Debug for KeyRange<Idx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(
            &Range {
                start: &self.start,
                end: &self.end,
            },
            f,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let range = KeyRange::new(10, 5);

        assert!(range.is_wrapping());
        assert!(range.contains(&0));
        assert!(!range.contains(&5));
        assert!(!range.contains(&7));
        assert!(!range.contains(&9));
        assert!(range.contains(&0));
        assert!(range.contains(&4));
        assert!(range.contains(&10));
        assert!(range.contains(&u64::MAX));

        let range = KeyRange::new(5, 10);

        assert!(!range.is_wrapping());
        assert!(!range.contains(&0));
        assert!(range.contains(&5));
        assert!(range.contains(&7));
        assert!(range.contains(&9));
        assert!(!range.contains(&0));
        assert!(!range.contains(&4));
        assert!(!range.contains(&10));
        assert!(!range.contains(&u64::MAX));
    }

    #[test]
    fn overlap() {
        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            // =====
            //      =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 15);

            assert!(!r1.is_overlapping(&r2));
            assert!(!r2.is_overlapping(&r1));
        }

        {
            //     =====
            // ====     ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 5);

            assert!(!r1.is_overlapping(&r2));
            assert!(!r2.is_overlapping(&r1));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            //       =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(7, 5);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            //       =====
            // =============
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            // =====     ====
            // =======  =====
            let r1 = KeyRange::new(10, 5);
            let r2 = KeyRange::new(9, 6);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }
    }

    #[test]
    fn extension() {
        {
            // =====
            //    =====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 13));
        }

        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(8, 13);

            r2.extend_end(&r1);

            assert_eq!(r2, KeyRange::new(8, 13));
        }

        {
            // =====
            //    =====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(8, 13);

            r2.extend_start(&r1);

            assert_eq!(r2, KeyRange::new(5, 13));
        }

        {
            //     =====
            // ======   ====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(10, 7);

            r2.extend_end(&r1);

            assert_eq!(r2, KeyRange::new(10, 10));
        }

        {
            //     =====
            // ======   ====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(10, 7);

            r2.extend_start(&r1);

            assert_eq!(r2, KeyRange::new(7, 7));
        }

        {
            //       =====
            // =============
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //       =====
            // =============
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }
    }
}
