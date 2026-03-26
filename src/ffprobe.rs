use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::time::Duration;
use tokio::process::Command;

// ─── Structures de désérialisation ffprobe ────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawFfprobeOutput {
    format:  RawFormat,
    streams: Vec<RawStream>,
}

#[derive(Debug, Deserialize)]
struct RawFormat {
    duration: String,
}

#[derive(Debug, Deserialize)]
struct RawStream {
    codec_type:   String,
    codec_name:   Option<String>,
    r_frame_rate: Option<String>, // ex: "25/1" ou "30000/1001"
    width:        Option<u32>,
    height:       Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RawFrames {
    frames: Vec<RawFrame>,
}

#[derive(Debug, Deserialize)]
struct RawFrame {
    // ffprobe retourne les timestamps en string pour éviter les pertes de
    // précision floating point
    pkt_pts_time: Option<String>,
    // Fallback si pkt_pts_time absent (cas de certains encodeurs)
    best_effort_timestamp_time: Option<String>,
}

// ─── Types publics ────────────────────────────────────────────────────────────

/// Métadonnées complètes d'une vidéo + timestamps des keyframes.
/// C'est ce qu'on stocke dans Valkey au moment du POST /session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VideoMetadata {
    /// Durée totale en secondes
    pub duration_secs: f64,
    /// Dimensions
    pub width:  u32,
    pub height: u32,
    /// Codec vidéo source (ex: "h264")
    pub video_codec: String,
    /// FPS réel (numérateur / dénominateur)
    pub fps_num: u32,
    pub fps_den: u32,
    /// Timestamps (en secondes) de chaque keyframe/IDR frame.
    /// Ces valeurs définissent les boundaries exactes des segments HLS.
    pub keyframe_timestamps: Vec<f64>,
}

impl VideoMetadata {
    /// FPS sous forme de f64 pour les calculs
    pub fn fps(&self) -> f64 {
        self.fps_num as f64 / self.fps_den as f64
    }

    /// Nombre total de keyframes (= nombre de segments possibles)
    pub fn keyframe_count(&self) -> usize {
        self.keyframe_timestamps.len()
    }
}

// ─── Appel principal ──────────────────────────────────────────────────────────

/// Récupère les métadonnées vidéo depuis une URL presignée R2.
///
/// Deux appels ffprobe distincts :
///   1. `-show_format -show_streams` → durée, codec, dimensions, fps
///   2. `-skip_frame nokey -show_frames` → timestamps de tous les keyframes
///
/// Pour un MP4 avec `moov` atom en tête (faststart), ffprobe ne télécharge
/// que les premiers kilobytes du fichier via HTTP Range — pas la vidéo entière.
pub async fn probe_video(presigned_url: &str) -> Result<VideoMetadata> {
    tracing::debug!("ffprobe: démarrage sur {}", &presigned_url[..60]);

    let (format_result, keyframes_result) = tokio::try_join!(
        probe_format(presigned_url),
        probe_keyframes(presigned_url),
    )?;

    Ok(VideoMetadata {
        duration_secs:       format_result.duration_secs,
        width:               format_result.width,
        height:              format_result.height,
        video_codec:         format_result.video_codec,
        fps_num:             format_result.fps_num,
        fps_den:             format_result.fps_den,
        keyframe_timestamps: keyframes_result,
    })
}

// ─── Probe format/streams ─────────────────────────────────────────────────────

struct FormatResult {
    duration_secs: f64,
    width:         u32,
    height:        u32,
    video_codec:   String,
    fps_num:       u32,
    fps_den:       u32,
}

async fn probe_format(url: &str) -> Result<FormatResult> {
    let output = Command::new("ffprobe")
        .args([
            "-v",           "quiet",
            "-print_format","json",
            "-show_format",
            "-show_streams",
            url,
        ])
        .output()
        .await
        .context("impossible de lancer ffprobe (est-il installé ?)")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffprobe format a échoué : {stderr}"));
    }

    let raw: RawFfprobeOutput = serde_json::from_slice(&output.stdout)
        .context("désérialisation ffprobe format")?;

    let duration_secs = raw.format.duration
        .parse::<f64>()
        .context("parsing durée ffprobe")?;

    // On prend le premier stream vidéo
    let video = raw.streams
        .iter()
        .find(|s| s.codec_type == "video")
        .ok_or_else(|| anyhow!("aucun stream vidéo trouvé"))?;

    let width  = video.width.unwrap_or(0);
    let height = video.height.unwrap_or(0);
    let codec  = video.codec_name.clone().unwrap_or_else(|| "unknown".into());

    let (fps_num, fps_den) = video.r_frame_rate
        .as_deref()
        .map(parse_rational)
        .unwrap_or((30, 1));

    Ok(FormatResult { duration_secs, width, height, video_codec: codec, fps_num, fps_den })
}

// ─── Probe keyframes ──────────────────────────────────────────────────────────

async fn probe_keyframes(url: &str) -> Result<Vec<f64>> {
    let output = Command::new("ffprobe")
        .args([
            "-v",            "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            "-show_packets",
            "-select_streams", "v:0",
            url,
        ])
        .output()
        .await
        .context("impossible de lancer ffprobe keyframes")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffprobe keyframes a échoué : {stderr}"));
    }

    let raw: RawFrames = serde_json::from_slice(&output.stdout)
        .context("désérialisation ffprobe frames")?;

    // On trie les timestamps pour garantir l'ordre (cas de streams désordonnés)
    let mut timestamps: Vec<f64> = raw.frames
        .iter()
        .filter_map(|f| {
            let ts_str = f.pkt_pts_time.as_deref()
                .or(f.best_effort_timestamp_time.as_deref())?;
            ts_str.parse::<f64>().ok()
        })
        .collect();

    if timestamps.is_empty() {
        return Err(anyhow!("aucun keyframe trouvé — le fichier est peut-être corrompu ou non-faststart"));
    }

    timestamps.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Le premier keyframe doit être à 0.0 (ou très proche)
    // On le normalise pour éviter des dérives d'affichage dans le M3U8
    if let Some(first) = timestamps.first().copied() {
        if first.abs() < 0.1 {
            timestamps[0] = 0.0;
        }
    }

    tracing::debug!(
        "ffprobe: {} keyframes trouvés sur {:.2}s",
        timestamps.len(),
        timestamps.last().unwrap_or(&0.0)
    );

    Ok(timestamps)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Parse "25/1" ou "30000/1001" en (num, den)
fn parse_rational(s: &str) -> (u32, u32) {
    let mut parts = s.splitn(2, '/');
    let num = parts.next().and_then(|v| v.parse().ok()).unwrap_or(30);
    let den = parts.next().and_then(|v| v.parse().ok()).unwrap_or(1);
    (num, den)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rational() {
        assert_eq!(parse_rational("25/1"),       (25, 1));
        assert_eq!(parse_rational("30000/1001"), (30000, 1001));
        assert_eq!(parse_rational("invalid"),    (30, 1));
    }

    #[test]
    fn test_fps() {
        let meta = VideoMetadata {
            duration_secs: 120.0,
            width: 1920, height: 1080,
            video_codec: "h264".into(),
            fps_num: 30000, fps_den: 1001,
            keyframe_timestamps: vec![0.0, 2.0, 4.0],
        };
        // 29.97fps
        assert!((meta.fps() - 29.97).abs() < 0.01);
    }
}