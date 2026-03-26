/// video_metadata.rs
///
/// Extraction des métadonnées vidéo + keyframes depuis une URL presignée R2.
/// Conçu pour être appelé une seule fois à l'upload (job background KEDA).
///
/// Stockage Valkey :
///   video:{key}:meta      → JSON  { duration_ms, width, height, codec, fps_num, fps_den }
///   video:{key}:keyframes → binaire compact (voir format ci-dessous)
///
/// Format binaire des keyframes :
///   [0..4]     u32 LE : nombre de keyframes (N)
///   [4..4+N*2] u16 LE × N : delta en millisecondes entre keyframes consécutifs
///
/// Gains vs Vec<f64> :
///   f64         : 8 bytes/keyframe
///   delta u16ms : 2 bytes/keyframe → 4× moins
///   Vidéo 2h, keyframe/2s = 3600 kf → ~7.2 KB au lieu de 28.8 KB
///   100k vidéos/an ≈ 700 MB Valkey

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

// ─── Types publics ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMeta {
    pub duration_ms: u32,
    pub width:       u32,
    pub height:      u32,
    pub codec:       String,
    pub fps_num:     u32,
    pub fps_den:     u32,
}

impl VideoMeta {
    pub fn duration_secs(&self) -> f64 { self.duration_ms as f64 / 1000.0 }
    pub fn fps(&self) -> f64 { self.fps_num as f64 / self.fps_den as f64 }
}

#[derive(Debug, Clone)]
pub struct Keyframes {
    pub timestamps_secs: Vec<f64>,
}

impl Keyframes {
    pub fn len(&self) -> usize { self.timestamps_secs.len() }
    pub fn is_empty(&self) -> bool { self.timestamps_secs.is_empty() }
}

// ─── Entrée principale ────────────────────────────────────────────────────────

/// Extrait métadonnées + keyframes, stocke dans Valkey.
///
/// probe_meta    → URL directe (lit uniquement le header, rapide)
/// probe_keyframes → reqwest stream → stdin ffprobe (pas de Range requests séquentielles)
///
/// Les deux tournent en parallèle via tokio::try_join!.
pub async fn extract_and_store(
    video_key:   &str,
    presign_url: &str,
    redis:       &mut impl redis::AsyncCommands,
) -> Result<(VideoMeta, Keyframes)> {

    let (meta, keyframes) = tokio::try_join!(
        probe_meta(presign_url),
        probe_keyframes_pipe(presign_url.clone()),
    )?;

    store_meta(video_key, &meta, redis).await?;
    store_keyframes(video_key, &keyframes, redis).await?;

    tracing::info!(
        "video {video_key} : {}×{} {:.2}fps {:.1}s {} keyframes",
        meta.width, meta.height, meta.fps(),
        meta.duration_secs(), keyframes.len()
    );

    Ok((meta, keyframes))
}

pub async fn get_meta(
    video_key: &str,
    redis:     &mut impl redis::AsyncCommands,
) -> Result<Option<VideoMeta>> {
    let raw: Option<String> = redis.get(format!("video:{video_key}:meta")).await?;
    Ok(raw.and_then(|s| serde_json::from_str(&s).ok()))
}

pub async fn get_keyframes(
    video_key: &str,
    redis:     &mut impl redis::AsyncCommands,
) -> Result<Option<Keyframes>> {
    let raw: Option<Vec<u8>> = redis.get(format!("video:{video_key}:keyframes")).await?;
    Ok(raw.map(|b| decode_keyframes(&b)).transpose()?)
}

// ─── probe_meta : URL directe, lit uniquement le header ──────────────────────

#[derive(Deserialize)]
struct RawProbeOutput {
    format:  RawFormat,
    streams: Vec<RawStream>,
}
#[derive(Deserialize)]
struct RawFormat { duration: String }
#[derive(Deserialize)]
struct RawStream {
    codec_type:   String,
    codec_name:   Option<String>,
    r_frame_rate: Option<String>,
    width:        Option<u32>,
    height:       Option<u32>,
}

async fn probe_meta(url: &str) -> Result<VideoMeta> {
    let out = Command::new("ffprobe")
        .args([
            "-v",            "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            url,
        ])
        .output()
        .await
        .context("lancement ffprobe meta")?;

    if !out.status.success() {
        return Err(anyhow!("ffprobe meta : {}", String::from_utf8_lossy(&out.stderr)));
    }

    let raw: RawProbeOutput = serde_json::from_slice(&out.stdout)
        .context("parsing ffprobe meta")?;

    let duration_ms = (raw.format.duration.parse::<f64>().context("durée")? * 1000.0).round() as u32;

    let video = raw.streams.iter()
        .find(|s| s.codec_type == "video")
        .ok_or_else(|| anyhow!("aucun stream vidéo"))?;

    let (fps_num, fps_den) = video.r_frame_rate.as_deref()
        .map(parse_rational)
        .unwrap_or((30, 1));

    Ok(VideoMeta {
        duration_ms,
        width:   video.width.unwrap_or(0),
        height:  video.height.unwrap_or(0),
        codec:   video.codec_name.clone().unwrap_or_else(|| "unknown".into()),
        fps_num,
        fps_den,
    })
}

// ─── probe_keyframes_pipe : reqwest stream → stdin ffprobe ───────────────────
//
// Au lieu de laisser ffprobe faire ses propres Range requests séquentielles
// vers R2 (→ latence × nb packets), on stream le fichier nous-mêmes via reqwest
// et on pipe les bytes vers stdin de ffprobe.
//
// ffprobe reçoit un flux linéaire continu → pas de Range requests, pas d'aller-
// retours réseau supplémentaires. Temps = durée du téléchargement pur.
//
// Architecture :
//
//   reqwest (download R2)
//      │  chunks
//      ▼
//   ffprobe stdin            ffprobe stdout
//      │                          │
//      │  [task séparée]          │  [task principale]
//      │                          ▼
//      │                    BufReader::lines()
//      │                    filtre flag 'K'
//      │                    → Vec<u32> timestamps_ms
//      │
//   ffprobe lit depuis "-" (stdin) et écrit les packets CSV sur stdout

async fn probe_keyframes_pipe(url: &str) -> Result<Keyframes> {
    // Lancer ffprobe en mode stdin ("-")
    let mut child = Command::new("ffprobe")
        .args([
            "-v",            "quiet",
            "-select_streams", "v:0",
            "-show_packets",
            "-show_entries", "packet=pts_time,flags",
            "-of",           "csv=p=0",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("lancement ffprobe keyframes")?;

    let mut stdin  = child.stdin.take()
        .ok_or_else(|| anyhow!("stdin ffprobe indisponible"))?;
    let stdout = child.stdout.take()
        .ok_or_else(|| anyhow!("stdout ffprobe indisponible"))?;

    let url_owned = url.to_string();

    // Task 1 : pompe reqwest → stdin ffprobe
    // On la spawn séparément pour ne pas bloquer la lecture stdout.
    let pump = tokio::spawn(async move {
        let client = reqwest::Client::builder()
            // Buffer de 8MB côté reqwest pour lisser les pics réseau
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build()?;

        let mut resp = client
            .get(url_owned)
            .send()
            .await?
            .error_for_status()?;

        while let Some(chunk) = resp.chunk().await? {
            if stdin.write_all(&chunk).await.is_err() {
                // ffprobe a fermé stdin (a tout ce dont il a besoin) → ok
                break;
            }
        }

        // Fermer stdin signale EOF à ffprobe → il termine proprement
        drop(stdin);
        Ok::<_, anyhow::Error>(())
    });

    // Task 2 (courante) : lire stdout ffprobe ligne par ligne
    // Format CSV : "pts_time,flags"  ex: "4.000000,K_"
    let mut reader = BufReader::new(stdout).lines();
    let mut timestamps_ms: Vec<u32> = Vec::with_capacity(2048);

    while let Some(line) = reader.next_line().await? {
        let mut parts = line.splitn(2, ',');
        let pts_str = match parts.next() { Some(s) => s, None => continue };
        let flags   = match parts.next() { Some(s) => s, None => continue };

        if !flags.starts_with('K') { continue; }

        if let Ok(pts) = pts_str.parse::<f64>() {
            timestamps_ms.push((pts * 1000.0).round() as u32);
        }
    }

    // Attendre la fin du process
    let status = child.wait().await.context("attente ffprobe")?;
    if !status.success() {
        return Err(anyhow!("ffprobe keyframes a échoué (code {:?})", status.code()));
    }

    // Vérifier que la pump n'a pas planté (erreur réseau R2 etc.)
    pump.await
        .context("task pump stdin paniquée")?
        .context("erreur download R2")?;

    if timestamps_ms.is_empty() {
        return Err(anyhow!("aucun keyframe trouvé"));
    }

    timestamps_ms.sort_unstable();

    if let Some(first) = timestamps_ms.first_mut() {
        if *first < 100 { *first = 0; }
    }

    Ok(Keyframes {
        timestamps_secs: timestamps_ms.iter().map(|&ms| ms as f64 / 1000.0).collect(),
    })
}

// ─── Encodage binaire compact ─────────────────────────────────────────────────

fn encode_keyframes(timestamps_secs: &[f64]) -> Vec<u8> {
    let n = timestamps_secs.len();
    let mut out = Vec::with_capacity(4 + n * 2);
    out.extend_from_slice(&(n as u32).to_le_bytes());

    let mut prev_ms = 0u32;
    for &ts in timestamps_secs {
        let ms    = (ts * 1000.0).round() as u32;
        let delta = ms.saturating_sub(prev_ms);
        if delta > 65535 {
            tracing::warn!("delta keyframe {delta}ms dépasse u16 max — GOP anormalement long");
        }
        out.extend_from_slice(&(delta.min(65535) as u16).to_le_bytes());
        prev_ms = ms;
    }
    out
}

fn decode_keyframes(bytes: &[u8]) -> Result<Keyframes> {
    if bytes.len() < 4 {
        return Err(anyhow!("buffer keyframes trop court"));
    }
    let n            = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + n * 2;

    if bytes.len() < expected_len {
        return Err(anyhow!(
            "buffer keyframes corrompu : attendu {expected_len} bytes, got {}",
            bytes.len()
        ));
    }

    let mut timestamps_secs = Vec::with_capacity(n);
    let mut acc_ms = 0u32;
    for i in 0..n {
        let offset = 4 + i * 2;
        let delta  = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as u32;
        acc_ms    += delta;
        timestamps_secs.push(acc_ms as f64 / 1000.0);
    }
    Ok(Keyframes { timestamps_secs })
}

// ─── Stockage Valkey ──────────────────────────────────────────────────────────

const TTL: u64 = 60 * 60 * 24 * 365; // 1 an

async fn store_meta(
    video_key: &str,
    meta:      &VideoMeta,
    redis:     &mut impl redis::AsyncCommands,
) -> Result<()> {
    redis.set_ex::<_, _, ()>(
        format!("video:{video_key}:meta"),
        serde_json::to_string(meta)?,
        TTL,
    ).await?;
    Ok(())
}

async fn store_keyframes(
    video_key: &str,
    keyframes: &Keyframes,
    redis:     &mut impl redis::AsyncCommands,
) -> Result<()> {
    let bytes = encode_keyframes(&keyframes.timestamps_secs);
    tracing::debug!(
        "keyframes {video_key}: {} kf → {} bytes ({:.1} bytes/kf)",
        keyframes.len(), bytes.len(),
        bytes.len() as f64 / keyframes.len().max(1) as f64
    );
    redis.set_ex::<_, _, ()>(
        format!("video:{video_key}:keyframes"),
        bytes,
        TTL,
    ).await?;
    Ok(())
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn parse_rational(s: &str) -> (u32, u32) {
    let mut p = s.splitn(2, '/');
    let num = p.next().and_then(|v| v.parse().ok()).unwrap_or(30);
    let den = p.next().and_then(|v| v.parse().ok()).unwrap_or(1);
    (num, den)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let original = vec![0.0, 4.0, 8.0, 12.083, 16.0, 20.0];
        let encoded  = encode_keyframes(&original);
        assert_eq!(encoded.len(), 4 + 6 * 2);

        let decoded = decode_keyframes(&encoded).unwrap();
        assert_eq!(decoded.len(), original.len());
        for (orig, dec) in original.iter().zip(decoded.timestamps_secs.iter()) {
            assert!((orig - dec).abs() < 0.001, "{orig} vs {dec}");
        }
    }

    #[test]
    fn size_estimate_100k_videos() {
        let bytes_per = 4 + 900 * 2; // 30min, kf/2s
        let total_gb  = (bytes_per * 100_000) as f64 / 1e9;
        assert!(total_gb < 0.2, "trop volumineux : {total_gb:.2} GB");
    }

    #[test]
    fn encode_first_keyframe_zero() {
        let bytes = encode_keyframes(&[0.0, 2.0, 4.0]);
        let first_delta = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        assert_eq!(first_delta, 0);
    }

    #[test]
    fn decode_corrupted_returns_err() {
        assert!(decode_keyframes(&[]).is_err());
        assert!(decode_keyframes(&[5, 0, 0, 0]).is_err());
    }
}