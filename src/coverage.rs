use recent_messages2::storage::CanonicalRecord;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageInterval {
    pub start_ms: i64,
    pub end_ms: i64,
    pub source: String,
}

#[derive(Clone, Copy, Debug)]
pub struct CoverageRequest {
    pub after_ms: Option<i64>,
    pub before_ms: Option<i64>,
    pub limit: usize,
    pub now_ms: i64,
    pub retention_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoverageAssessment {
    pub sufficient: bool,
    pub required_start_ms: i64,
    pub required_end_ms: i64,
    pub gaps: Vec<(i64, i64)>,
}

pub fn assess_coverage(
    records: &[CanonicalRecord],
    intervals: &[CoverageInterval],
    request: CoverageRequest,
) -> CoverageAssessment {
    let required_end_ms = request
        .before_ms
        .unwrap_or(request.now_ms)
        .min(request.now_ms);
    if request.limit == 0
        || request
            .after_ms
            .is_some_and(|after| after >= required_end_ms)
    {
        return CoverageAssessment {
            sufficient: true,
            required_start_ms: required_end_ms,
            required_end_ms,
            gaps: Vec::new(),
        };
    }

    let horizon_start = required_end_ms.saturating_sub(request.retention_ms);
    let requested_start = request.after_ms.unwrap_or(horizon_start).max(horizon_start);
    let required_start_ms = if records.len() >= request.limit {
        records
            .iter()
            .rev()
            .take(request.limit)
            .map(|record| record.received_at_ms)
            .min()
            .unwrap_or(required_end_ms)
            .max(requested_start)
    } else {
        requested_start
    };

    let gaps = uncovered_intervals(intervals, required_start_ms, required_end_ms);
    CoverageAssessment {
        sufficient: gaps.is_empty(),
        required_start_ms,
        required_end_ms,
        gaps,
    }
}

pub fn merge_intervals(
    intervals: impl IntoIterator<Item = CoverageInterval>,
) -> Vec<CoverageInterval> {
    let mut intervals = intervals.into_iter().collect::<Vec<_>>();
    intervals.sort_by_key(|interval| (interval.start_ms, interval.end_ms));
    let mut merged: Vec<CoverageInterval> = Vec::new();
    for interval in intervals {
        if interval.end_ms < interval.start_ms {
            continue;
        }
        if let Some(previous) = merged.last_mut()
            && interval.start_ms <= previous.end_ms.saturating_add(1)
        {
            previous.end_ms = previous.end_ms.max(interval.end_ms);
            if !previous
                .source
                .split(',')
                .any(|source| source == interval.source)
            {
                previous.source.push(',');
                previous.source.push_str(&interval.source);
            }
            continue;
        }
        merged.push(interval);
    }
    merged
}

fn uncovered_intervals(
    intervals: &[CoverageInterval],
    required_start_ms: i64,
    required_end_ms: i64,
) -> Vec<(i64, i64)> {
    if required_start_ms >= required_end_ms {
        return Vec::new();
    }
    let merged = merge_intervals(intervals.iter().cloned());
    let mut cursor = required_start_ms;
    let mut gaps = Vec::new();
    for interval in merged {
        if interval.end_ms < cursor || interval.start_ms > required_end_ms {
            continue;
        }
        if interval.start_ms > cursor {
            gaps.push((cursor, interval.start_ms.min(required_end_ms)));
        }
        cursor = cursor.max(interval.end_ms);
        if cursor >= required_end_ms {
            break;
        }
    }
    if cursor < required_end_ms {
        gaps.push((cursor, required_end_ms));
    }
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use recent_messages2::storage::SourceFidelity;

    fn record(received_at_ms: i64) -> CanonicalRecord {
        CanonicalRecord {
            channel_key: "channel".to_owned(),
            event_at_ms: received_at_ms,
            received_at_ms,
            event_key: [u8::try_from(received_at_ms).unwrap_or_default(); 32],
            source_id: "test".to_owned(),
            fidelity: SourceFidelity::DirectIrc,
            raw_irc: Vec::new(),
        }
    }

    fn interval(start_ms: i64, end_ms: i64) -> CoverageInterval {
        CoverageInterval {
            start_ms,
            end_ms,
            source: "direct-irc".to_owned(),
        }
    }

    #[test]
    fn a_recent_join_does_not_make_a_short_result_complete() {
        let assessment = assess_coverage(
            &[record(95)],
            &[interval(90, 100)],
            CoverageRequest {
                after_ms: None,
                before_ms: None,
                limit: 800,
                now_ms: 100,
                retention_ms: 100,
            },
        );
        assert!(!assessment.sufficient);
        assert_eq!(assessment.gaps, vec![(0, 90)]);
    }

    #[test]
    fn enough_newest_records_only_require_continuity_from_the_oldest_returned() {
        let records = (91..=100).map(record).collect::<Vec<_>>();
        let assessment = assess_coverage(
            &records,
            &[interval(90, 100)],
            CoverageRequest {
                after_ms: None,
                before_ms: None,
                limit: 5,
                now_ms: 100,
                retention_ms: 100,
            },
        );
        assert!(assessment.sufficient);
        assert_eq!(assessment.required_start_ms, 96);
    }

    #[test]
    fn a_gap_inside_the_returned_window_is_incomplete() {
        let records = (91..=100).map(record).collect::<Vec<_>>();
        let assessment = assess_coverage(
            &records,
            &[interval(90, 97), interval(99, 100)],
            CoverageRequest {
                after_ms: None,
                before_ms: None,
                limit: 5,
                now_ms: 100,
                retention_ms: 100,
            },
        );
        assert!(!assessment.sufficient);
        assert_eq!(assessment.gaps, vec![(97, 99)]);
    }

    #[test]
    fn explicit_interval_can_prove_an_empty_result() {
        let assessment = assess_coverage(
            &[],
            &[interval(50, 100)],
            CoverageRequest {
                after_ms: Some(50),
                before_ms: Some(100),
                limit: 800,
                now_ms: 100,
                retention_ms: 100,
            },
        );
        assert!(assessment.sufficient);
    }
}
