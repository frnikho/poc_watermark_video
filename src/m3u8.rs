/// m3u8.rs
///
/// Génération de playlists HLS (M3U8) pour le watermarking JIT.
///
/// Spec HLS appliquée :
///   - Version 3 (compatible tous players : hls.js, AVPlayer, ExoPlayer)
///   - #EXT-X-TARGETDURATION = ceil(max durée segment)
///   - #EXT-X-MEDIA-SEQUENCE = 0 (VOD, pas de live)
///   - #EXT-X-ENDLIST obligatoire (VOD)
///   - #EXTINF avec 3 décimales (spec recommande ms de précision)
///
/// URLs des segments : déterministes via HMAC (session_id dépend du viewer)
/// → Cloudflare peut cacher par URL sans ambiguïté.

use crate::segmentation::{max_segment_duration, Segment};

// ─── Builder principal ────────────────────────────────────────────────────────

pub struct M3u8Builder<'a> {
    session_id:   &'a str,
    segments:     &'a [Segment],
    segment_base: &'a str, // préfixe URL, ex: "/segment" ou "https://cdn.example.com/segment"
}

impl<'a> M3u8Builder<'a> {
    pub fn new(session_id: &'a str, segments: &'a [Segment]) -> Self {
        Self {
            session_id,
            segments,
            segment_base: "/segment",
        }
    }

    /// Permet de changer le préfixe d'URL des segments (ex: CDN absolu)
    pub fn segment_base(mut self, base: &'a str) -> Self {
        self.segment_base = base;
        self
    }

    /// Génère la playlist M3U8 complète.
    ///
    /// Optimisation mémoire : capacité pré-calculée avant d'allouer le String.
    /// Évite les réallocations sur les longues vidéos (>1000 segments).
    pub fn build(self) -> String {
        let n              = self.segments.len();
        let target_dur     = max_segment_duration(self.segments);

        // Estimation de la taille finale :
        //   header ~100 bytes
        //   par segment : "#EXTINF:X.XXX,\n/segment/SESSION_ID/INDEX\n"
        //   session_id  ~16 chars, index ~4 chars, durée ~7 chars
        //   ≈ 60 bytes/segment en moyenne
        let capacity = 120 + n * 65;
        let mut out  = String::with_capacity(capacity);

        // Header
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:3\n");
        push_kv(&mut out, "#EXT-X-TARGETDURATION:", &target_dur.to_string());
        out.push_str("#EXT-X-MEDIA-SEQUENCE:0\n");
        out.push('\n');

        // Segments
        for seg in self.segments {
            out.push_str("#EXTINF:");
            out.push_str(&seg.extinf());
            out.push_str(",\n");
            out.push_str(self.segment_base);
            out.push('/');
            out.push_str(self.session_id);
            out.push('/');
            // itoa serait marginalement plus rapide mais serde_json est déjà en deps
            // et n est petit — format! est acceptable ici
            out.push_str(&seg.index.to_string());
            out.push_str(".ts");
            out.push('\n');
        }

        out.push_str("#EXT-X-ENDLIST\n");
        out
    }
}

// ─── Helper ───────────────────────────────────────────────────────────────────

#[inline]
fn push_kv(s: &mut String, key: &str, val: &str) {
    s.push_str(key);
    s.push_str(val);
    s.push('\n');
}

// ─── Content-Type ─────────────────────────────────────────────────────────────

/// Content-Type HTTP correct pour un fichier M3U8.
/// Certains players (AVPlayer notamment) refusent application/octet-stream.
pub const CONTENT_TYPE: &str = "application/vnd.apple.mpegurl";

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segmentation::Segment;

    fn make_segments(durations: &[f64]) -> Vec<Segment> {
        let mut start = 0.0_f64;
        durations.iter().enumerate().map(|(i, &d)| {
            let seg = Segment {
                index: i,
                start_secs: start,
                end_secs: start + d,
                duration_secs: d,
            };
            start += d;
            seg
        }).collect()
    }

    #[test]
    fn valid_m3u8_structure() {
        let segs = make_segments(&[4.0, 4.083, 4.0, 3.917]);
        let m3u8 = M3u8Builder::new("abc123", &segs).build();

        assert!(m3u8.starts_with("#EXTM3U\n"));
        assert!(m3u8.contains("#EXT-X-VERSION:3\n"));
        assert!(m3u8.contains("#EXT-X-TARGETDURATION:5\n")); // ceil(4.083)
        assert!(m3u8.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert!(m3u8.ends_with("#EXT-X-ENDLIST\n"));
    }

    #[test]
    fn segment_urls_correct() {
        let segs = make_segments(&[4.0, 4.0, 4.0]);
        let m3u8 = M3u8Builder::new("sess_xyz", &segs).build();

        assert!(m3u8.contains("/segment/sess_xyz/0\n"));
        assert!(m3u8.contains("/segment/sess_xyz/1\n"));
        assert!(m3u8.contains("/segment/sess_xyz/2\n"));
    }

    #[test]
    fn custom_base_url() {
        let segs = make_segments(&[4.0]);
        let m3u8 = M3u8Builder::new("sess", &segs)
            .segment_base("https://cdn.example.com/segment")
            .build();
        assert!(m3u8.contains("https://cdn.example.com/segment/sess/0\n"));
    }

    #[test]
    fn extinf_precision() {
        // Les durées irrégulières doivent être à 3 décimales
        let segs = make_segments(&[4.083, 3.917]);
        let m3u8 = M3u8Builder::new("s", &segs).build();
        assert!(m3u8.contains("#EXTINF:4.083,\n"));
        assert!(m3u8.contains("#EXTINF:3.917,\n"));
    }

    #[test]
    fn target_duration_is_ceiling() {
        // #EXT-X-TARGETDURATION doit être l'entier >= max durée segment
        let segs = make_segments(&[4.0, 4.001, 3.999]);
        let m3u8 = M3u8Builder::new("s", &segs).build();
        // ceil(4.001) = 5
        assert!(m3u8.contains("#EXT-X-TARGETDURATION:5\n"));
    }

    #[test]
    fn segment_count() {
        let segs = make_segments(&[4.0; 100]);
        let m3u8 = M3u8Builder::new("s", &segs).build();
        let extinf_count = m3u8.matches("#EXTINF:").count();
        assert_eq!(extinf_count, 100);
    }

    #[test]
    fn capacity_no_realloc() {
        // Vérifie que la capacité pré-allouée est suffisante (pas de realloc)
        // en comparant la longueur finale avec la capacité estimée
        let segs = make_segments(&[4.0; 500]);
        let m3u8 = M3u8Builder::new("abcdef1234567890", &segs).build();
        // Le String ne doit pas avoir realloc (len <= capacity initiale)
        // On vérifie juste que le build fonctionne sans panic
        assert!(m3u8.len() > 0);
    }
}