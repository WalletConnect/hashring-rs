use super::KeyRange;

pub struct MergedRanges<K, I: IntoIterator<Item = KeyRange<K>>> {
    values: I,
    last: Option<KeyRange<K>>,
}

/// Sorts the provided ranges and returns an iterator that yields merged
/// (deoverlapped) ranges.
pub fn merge_ranges<K, I>(
    ranges: I,
) -> MergedRanges<K, <Vec<KeyRange<K>> as IntoIterator>::IntoIter>
where
    K: PartialOrd + Ord + Clone,
    I: Into<Vec<KeyRange<K>>>,
{
    let mut ranges = ranges.into();
    ranges.sort_by(|a, b| a.start.cmp(&b.start));

    merge_ranges_sorted(ranges)
}

/// Returns an iterator that yields merged (deoverlapped) ranges from the input.
/// The input iterator must yield ranges sorted on by `start` ascending.
pub fn merge_ranges_sorted<K, T>(ranges: T) -> MergedRanges<K, <T as IntoIterator>::IntoIter>
where
    K: PartialOrd + Ord + Clone,
    T: IntoIterator<Item = KeyRange<K>>,
{
    let mut iterator = ranges.into_iter();
    let last = iterator.next();

    MergedRanges {
        values: iterator,
        last,
    }
}

impl<K, I> Iterator for MergedRanges<K, I>
where
    K: PartialOrd + Ord + Clone,
    I: Iterator<Item = KeyRange<K>>,
{
    type Item = KeyRange<K>;

    fn next(&mut self) -> Option<KeyRange<K>> {
        if let Some(mut last) = self.last.clone() {
            for mut next in &mut self.values {
                if last.is_wrapping() {
                    if next.is_wrapping() {
                        // If both `last` and `next` are wrapping, extend `last` and keep going.
                        last.extend_end(&next);
                    }

                    // If `last` is wrapping and `next` isn't, then it's fully
                    // contained within `last and we don't need to extend.
                } else {
                    if next.is_wrapping() {
                        // If `last` is not wrapping and `next` is wrapping, we swap them and extend
                        // start accordingly.
                        if last.end >= next.start {
                            next.extend_start(&last);
                            last = next;
                        } else {
                            self.last = Some(next);

                            return Some(last);
                        }
                    } else {
                        // Both `last` and `next` are non-wrapping, so extend `last` if they
                        // overlap.
                        if last.end >= next.start {
                            last.extend_end(&next);
                        } else {
                            self.last = Some(next);

                            return Some(last);
                        }
                    }
                }
            }

            self.last = None;

            return Some(last);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::ops::Range};

    fn r(range: Range<u64>) -> KeyRange<u64> {
        range.into()
    }

    #[test]
    fn merge_regular_ranges() {
        let ranges = [r(3..6), r(8..10), r(2..5), r(1..4)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(1..6), r(8..10)]);

        let ranges = [r(3..6), r(8..10), r(2..5), r(1..4), r(0..20)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(0..20)]);
    }

    #[test]
    fn merge_wrapping_ranges() {
        let ranges = [r(25..30), r(10..5), r(11..4)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(10..5)]);
    }
}
