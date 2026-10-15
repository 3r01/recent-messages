use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct CachePlacement {
    local: Arc<str>,
    members: Arc<[String]>,
    copies: usize,
}

#[derive(Debug, Error)]
#[error("cache placement requires unique member names, the local member, and 1..=members copies")]
pub struct PlacementError;

impl CachePlacement {
    pub fn new(
        local: String,
        mut members: Vec<String>,
        copies: usize,
    ) -> Result<Self, PlacementError> {
        members.sort();
        if members.is_empty()
            || copies == 0
            || copies > members.len()
            || !members.contains(&local)
            || members.windows(2).any(|p| p[0] == p[1])
            || members.iter().any(|m| {
                m.is_empty()
                    || m.len() > 64
                    || !m
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            })
        {
            return Err(PlacementError);
        }
        Ok(Self {
            local: local.into(),
            members: members.into(),
            copies,
        })
    }

    #[must_use]
    pub fn owners(&self, channel: &str) -> Vec<&str> {
        let channel = channel.to_ascii_lowercase();
        let mut ranked: Vec<_> = self
            .members
            .iter()
            .map(|member| {
                let mut hash = blake3::Hasher::new_derive_key("recent-messages cache placement v1");
                hash.update(member.as_bytes());
                hash.update(&[0]);
                hash.update(channel.as_bytes());
                (*hash.finalize().as_bytes(), member.as_str())
            })
            .collect();
        ranked.sort_unstable_by(|a, b| b.cmp(a));
        ranked
            .into_iter()
            .take(self.copies)
            .map(|(_, member)| member)
            .collect()
    }

    #[must_use]
    pub fn retains(&self, channel: &str) -> bool {
        self.owners(channel).contains(&self.local.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_agrees_is_overlapping_and_spreads_storage() {
        let members = vec![
            "a".to_owned(),
            "b".to_owned(),
            "c".to_owned(),
            "d".to_owned(),
        ];
        let nodes: Vec<_> = members
            .iter()
            .map(|n| CachePlacement::new(n.clone(), members.clone(), 2).unwrap())
            .collect();
        let mut counts = [0; 4];
        for i in 0..1000 {
            let channel = format!("channel{i}");
            let owners = nodes[0].owners(&channel);
            assert_eq!(owners.len(), 2);
            for (j, node) in nodes.iter().enumerate() {
                assert_eq!(owners, node.owners(&channel.to_uppercase()));
                if node.retains(&channel) {
                    counts[j] += 1;
                }
            }
        }
        assert_eq!(counts.iter().sum::<usize>(), 2000);
        assert!(counts.iter().all(|n| (400..600).contains(n)));
        let reversed =
            CachePlacement::new("a".to_owned(), members.into_iter().rev().collect(), 2).unwrap();
        assert_eq!(nodes[0].owners("channel1"), reversed.owners("channel1"));
    }

    #[test]
    fn adding_a_member_preserves_owners_unless_the_new_member_wins() {
        let before =
            CachePlacement::new("a".into(), vec!["a".into(), "b".into(), "c".into()], 2).unwrap();
        let after = CachePlacement::new(
            "a".into(),
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
            2,
        )
        .unwrap();
        for i in 0..1000 {
            let channel = format!("channel{i}");
            let new = after.owners(&channel);
            if !new.contains(&"d") {
                assert_eq!(before.owners(&channel), new);
            }
        }
    }
}
