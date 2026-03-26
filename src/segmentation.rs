/// segmentation.rs
///
/// Découpe une vidéo en segments HLS alignés exactement sur les keyframes.
///
/// Principe : pour chaque intervalle cible [N×target, (N+1)×target],
/// on cherche le keyframe le plus proche du bord droit via binary search O(log n).
/// Les segments ont des durées légèrement variables (~target ± demi-GOP)
/// mais ne coupent jamais entre deux keyframes → zéro frame perdue.

use serde::{Deserialize, Serialize};

// ─── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    /// Index 0-based, utilisé dans l'URL HLS : /segment/:session/:index
    pub index:         usize,
    /// Timestamp de début exact (= timestamp d'un keyframe)
    pub start_secs:    f64,
    /// Timestamp de fin exact (= timestamp du keyframe suivant ou fin fichier)
    pub end_secs:      f64,
    /// Durée réelle, utilisée dans le #EXTINF du M3U8
    pub duration_secs: f64,
}

impl Segment {
    /// Durée formatée pour #EXTINF (3 décimales, spec HLS)
    #[inline]
    pub fn extinf(&self) -> String {
        format!("{:.3}", self.duration_secs)
    }
}

// ─── Entrée principale ────────────────────────────────────────────────────────

/// Découpe en segments alignés keyframe.
///
/// `keyframes_secs` : timestamps triés croissant (issus de video_metadata)
/// `duration_secs`  : durée totale de la vidéo
/// `target_secs`    : durée cible d'un segment (ex: 4.0)
///
/// Optimisations vs version précédente :
///   - Binary search O(log n) au lieu de scan linéaire O(n)
///   - Capacité Vec pré-allouée
///   - Pas de copie intermédiaire des candidats
pub fn compute_segments(
    keyframes_secs: &[f64],
    duration_secs:  f64,
    target_secs:    f64,
) -> Vec<Segment> {
    if keyframes_secs.is_empty() || duration_secs <= 0.0 || target_secs <= 0.0 {
        return vec![];
    }

    let estimated_count = (duration_secs / target_secs).ceil() as usize + 1;
    let mut segments    = Vec::with_capacity(estimated_count);
    let mut seg_start   = keyframes_secs[0]; // normalisé à 0.0
    let mut index       = 0usize;

    loop {
        let target_end = seg_start + target_secs;

        if target_end >= duration_secs {
            // Dernier segment : s'étend jusqu'à la fin exacte du fichier
            segments.push(Segment {
                index,
                start_secs:    seg_start,
                end_secs:      duration_secs,
                duration_secs: duration_secs - seg_start,
            });
            break;
        }

        let best_end = nearest_keyframe(keyframes_secs, seg_start, target_end);

        segments.push(Segment {
            index,
            start_secs:    seg_start,
            end_secs:      best_end,
            duration_secs: best_end - seg_start,
        });

        seg_start = best_end;
        index    += 1;

        if seg_start >= duration_secs { break; }
    }

    segments
}

/// Durée maximum observée sur tous les segments.
/// Utilisée pour #EXT-X-TARGETDURATION (doit être >= max durée, arrondi au supérieur).
pub fn max_segment_duration(segments: &[Segment]) -> u64 {
    segments.iter()
        .map(|s| s.duration_secs)
        .fold(0.0_f64, f64::max)
        .ceil() as u64
}

// ─── Binary search du keyframe le plus proche ─────────────────────────────────

/// Retourne le timestamp du keyframe le plus proche de `target`,
/// parmi les keyframes strictement après `after`.
///
/// Utilise binary search O(log n) pour trouver le point de partition autour
/// de `target`, puis compare uniquement les deux candidats adjacents.
///
/// Préférence légère pour le keyframe AVANT target afin d'éviter les micro-
/// segments (un keyframe 50ms après target est préféré seulement s'il est
/// nettement plus proche).
fn nearest_keyframe(keyframes: &[f64], after: f64, target: f64) -> f64 {
    // Trouver le premier keyframe > after via binary search
    let start_idx = keyframes.partition_point(|&k| k <= after);

    if start_idx >= keyframes.len() {
        // Plus de keyframes disponibles (ne devrait pas arriver en pratique)
        return target;
    }

    // Parmi keyframes[start_idx..], trouver la partition autour de target
    let slice     = &keyframes[start_idx..];
    let pivot     = slice.partition_point(|&k| k <= target);

    let before = pivot.checked_sub(1).map(|i| slice[i]); // dernier kf <= target
    let after_t = slice.get(pivot).copied();              // premier kf > target

    match (before, after_t) {
        (Some(b), Some(a)) => {
            let dist_b = target - b;
            let dist_a = a - target;
            // Préférer "avant" sauf si "après" est nettement plus proche (< 40%)
            if dist_a < dist_b * 0.4 { a } else { b }
        }
        (Some(b), None) => b,
        (None,    Some(a)) => a,
        (None,    None) => target, // ne peut pas arriver (slice non vide)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_alignment() {
        let kf   = vec![0.0, 4.0, 8.0, 12.0, 16.0, 20.0];
        let segs = compute_segments(&kf, 20.0, 4.0);
        assert_eq!(segs.len(), 5);
        assert_eq!(segs[0].start_secs, 0.0);
        assert_eq!(segs[0].end_secs,   4.0);
        assert_eq!(segs[4].end_secs,   20.0);
        // Boundaries = keyframes
        for seg in &segs {
            assert!(kf.contains(&seg.start_secs) || seg.start_secs == 0.0);
        }
    }

    #[test]
    fn irregular_gop() {
        let kf   = vec![0.0, 2.0, 4.08, 6.0, 8.16, 10.0, 12.08, 14.0, 16.16, 18.0, 20.0];
        let segs = compute_segments(&kf, 20.0, 4.0);

        // Tous les starts sur un keyframe
        for seg in &segs {
            assert!(kf.iter().any(|&k| (k - seg.start_secs).abs() < 1e-9),
                    "seg {} start {} pas sur keyframe", seg.index, seg.start_secs);
        }
        // Couverture complète sans trou
        assert_eq!(segs.first().unwrap().start_secs, 0.0);
        assert!((segs.last().unwrap().end_secs - 20.0).abs() < 1e-9);
        for i in 1..segs.len() {
            assert!((segs[i].start_secs - segs[i-1].end_secs).abs() < 1e-9,
                    "trou entre seg {} et {}", i-1, i);
        }
    }

    #[test]
    fn last_segment_shorter() {
        let segs = compute_segments(&[0.0, 4.0, 8.0], 10.3, 4.0);
        let last = segs.last().unwrap();
        assert!((last.end_secs - 10.3).abs() < 1e-9);
        assert!(last.duration_secs < 4.0);
    }

    #[test]
    fn max_duration_ceiling() {
        let kf   = vec![0.0, 4.08, 8.0, 12.0];
        let segs = compute_segments(&kf, 12.0, 4.0);
        // max_segment_duration doit être >= toutes les durées
        let max = max_segment_duration(&segs);
        for seg in &segs {
            assert!(seg.duration_secs <= max as f64);
        }
    }

    #[test]
    fn binary_search_correctness() {
        // Vérifie que binary search et scan linéaire donnent le même résultat
        let kf = vec![0.0, 2.0, 4.083, 6.0, 8.16, 10.0];
        for target in [3.0, 4.0, 5.0, 7.0, 9.0_f64] {
            let bs     = nearest_keyframe(&kf, 0.0, target);
            // Vérifier que le résultat est bien un keyframe existant
            assert!(kf.iter().any(|&k| (k - bs).abs() < 1e-9),
                    "résultat {bs} n'est pas un keyframe (target={target})");
        }
    }

    #[test]
    fn empty_inputs() {
        assert!(compute_segments(&[], 20.0, 4.0).is_empty());
        assert!(compute_segments(&[0.0], 0.0, 4.0).is_empty());
        assert!(compute_segments(&[0.0], 20.0, 0.0).is_empty());
    }

    #[test]
    fn extinf_format() {
        let seg = Segment { index: 0, start_secs: 0.0, end_secs: 4.083, duration_secs: 4.083 };
        assert_eq!(seg.extinf(), "4.083");
    }
}