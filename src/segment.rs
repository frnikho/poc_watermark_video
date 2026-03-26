/// segment.rs
///
/// Génération JIT d'un segment HLS watermarké via FFmpeg.
///
/// Flow :
///   1. seek `-ss start_secs` avant `-i url` → FFmpeg envoie une HTTP Range
///      request directement sur R2, positionnée sur le bon keyframe
///   2. `-t duration_secs` → traite exactement ce segment
///   3. `-vf drawtext` → watermark nom/prénom
///   4. stdout → streamé directement dans la réponse HTTP (chunked)
///
/// Depuis que nos segments sont alignés sur des keyframes (segmentation.rs),
/// le seek `-ss` tombe exactement sur un IDR frame → FFmpeg n'a aucune frame
/// à décoder "à blanc" avant le début du segment → 0 frame perdue, seek rapide.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio_util::io::ReaderStream;

// ─── Handle FFmpeg ────────────────────────────────────────────────────────────

/// Handle sur un process FFmpeg en cours, avec kill automatique au drop.
///
/// Si le client HTTP se déconnecte (player arrête, refresh...), axum drop
/// le body stream → FfmpegHandle est drop → FFmpeg est kill.
/// Évite les process zombies qui continueraient à encoder pour rien.
pub struct FfmpegHandle {
    child: Child,
}

impl FfmpegHandle {
    /// Spawn FFmpeg et retourne le handle + un stream des bytes de sortie.
    ///
    /// `presign_url`  : URL presignée R2 du fichier source
    /// `start_secs`   : timestamp de début du segment (= timestamp d'un keyframe)
    /// `duration_secs`: durée du segment
    /// `firstname`    : prénom du viewer (watermark)
    /// `lastname`     : nom du viewer (watermark)
    pub async fn spawn(
        presign_url:   &str,
        start_secs:    f64,
        duration_secs: f64,
        firstname:     &str,
        lastname:      &str,
    ) -> Result<(Self, impl futures_core::Stream<Item = std::io::Result<Bytes>>)> {

        // Texte du watermark — échapper les caractères spéciaux ffmpeg drawtext
        let watermark_text = escape_drawtext(&format!("{firstname} {lastname}"));

        // Filtre drawtext :
        //   - coin haut-gauche avec marge de sécurité
        //   - ombre portée pour lisibilité sur fond clair ou sombre
        //   - alpha 0.6 pour ne pas gêner la lecture
        let drawtext = format!(
            "drawtext=\
             text='{watermark_text}':\
             fontsize=28:\
             fontcolor=white@0.6:\
             x=24:\
             y=24:\
             shadowcolor=black@0.5:\
             shadowx=2:\
             shadowy=2"
        );

        let mut child = Command::new("ffmpeg")
            .args([
                "-loglevel", "error",
                // Input seek (avant -i = seek rapide sur keyframe)
                "-ss", &format!("{start_secs:.6}"),
                "-i",  presign_url,
                "-t",  &format!("{duration_secs:.6}"),

                // Watermark
                "-vf", &drawtext,

                // Vidéo : pas de forçage de fps, on garde le fps source
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-crf", "28",
                // IDR frame obligatoire au début du segment (HLS = segments indépendants)
                "-force_key_frames", "expr:eq(n,0)",

                // Audio : copie directe si la source est déjà en AAC (cas MP4 standard).
                // Évite le ré-encodage AAC qui introduit un encoder delay (~21ms)
                // causant un léger gréssillement à chaque jonction de segments.
                "-c:a", "copy",

                // Offset de sortie = position réelle dans la timeline globale.
                // Chaque segment a ses PTS qui commencent à start_secs (et non à 0),
                // ce qui permet à hls.js de placer les segments sans recalcul,
                // garantit la continuité audio entre segments, et empêche le
                // player de sauter des segments courts.
                "-output_ts_offset", &format!("{start_secs:.6}"),

                // MPEG-TS sur stdout
                "-f", "mpegts",
                "pipe:1",
            ])

       /* let mut child = Command::new("ffmpeg")
            .args([
                "-loglevel", "error",               // Réduit le bruit dans stderr
                "-ss", &start_secs.to_string(),
                "-i",  presign_url,
                "-t",  &duration_secs.to_string(),
                // Filtre Watermark
                "-vf", &format!("drawtext=text='{} {}':x=10:y=10:fontsize=24:fontcolor=white", firstname, lastname),

                // VIDÉO : On force le profil baseline et le format bitstream pour le TS
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-profile:v", "baseline",           // Plus compatible avec les vieux décodeurs
                "-level", "3.0",
                "-pix_fmt", "yuv420p",              // Assure le format de pixels standard
                "-bsf:v", "h264_mp4toannexb",       // FORCE le format Annex B pour le MPEG-TS

                // AUDIO : TRANSCODAGE OBLIGATOIRE
                "-c:a", "aac",
                "-b:a", "128k",
                "-ac", "2",                         // Force le stéréo

                // MUXER : Configuration spécifique HLS/TS
                "-f", "mpegts",
                "-mpegts_flags", "resend_headers",  // Répète PAT/PMT pour VLC
                "-muxdelay", "0",
                "pipe:1",
            ])*/

       /*let mut child = Command::new("ffmpeg")
            .args([
                "-copyts",
                // ── Input ──────────────────────────────────────────────────
                // -ss AVANT -i = seek rapide (index-based, pas de décodage)
                // FFmpeg envoie une Range request R2 positionnée sur le keyframe
                "-ss",    &format!("{start_secs:.6}"),
                "-i",     presign_url,

                // ── Durée exacte du segment ────────────────────────────────
                "-t",     &format!("{duration_secs:.6}"),

                // ── Watermark ─────────────────────────────────────────────
                "-vf",    &drawtext,

                // ── Encodage vidéo ─────────────────────────────────────────
                // ultrafast + crf 28 : ~40% CPU vs veryfast, qualité suffisante
                // pour un watermark de sécurité servi en privé
                "-c:v",   "libx264",
                "-preset","ultrafast",
                "-crf",   "28",

                // Force un IDR frame au tout début du segment.
                // Indispensable pour que le player puisse décoder le segment
                // indépendamment des précédents (HLS = segments indépendants).
                "-force_key_frames", "expr:eq(n,0)",

                // ── Audio ──────────────────────────────────────────────────
                // Copie sans ré-encodage : économise ~30% CPU supplémentaire
                "-c:a",   "copy",

                // ── Timestamps ────────────────────────────────────────────
                // Après un seek, les timestamps internes peuvent être négatifs.
                // make_zero les rebase à 0 → player ne dérive pas.
                "-avoid_negative_ts", "make_zero",

                // ── Output : MPEG-TS sur stdout ────────────────────────────
                // MPEG-TS est le format natif des segments HLS (.ts)
                // pipe:1 = stdout (pas de fichier intermédiaire)
                "-f",     "mpegts",
                "-muxdelay", "0",                  // Pas de délai de multiplexage
                "-",

                // Pas de bannière ni de stats dans stderr
                "-hide_banner",
                "-nostats",
            ])*/
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()) // capturé pour les logs d'erreur
            .stdin(Stdio::null())
            .spawn()
            .context("impossible de spawner ffmpeg — est-il installé ?")?;

        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow!("stdout ffmpeg indisponible"))?;

        // Capturer stderr en tâche de fond pour logger les erreurs éventuelles
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut buf = String::new();
                if stderr.read_to_string(&mut buf).await.is_ok() && !buf.is_empty() {
                    tracing::debug!("ffmpeg stderr: {}", buf.trim());
                }
            });
        }

        let stream = ReaderStream::new(stdout);
        Ok((Self { child }, stream))
    }

    /// Attend la fin du process FFmpeg et vérifie le code de retour.
    /// À appeler après avoir consommé tout le stream.
    pub async fn wait(mut self) -> Result<()> {
        let status = self.child.wait().await.context("attente ffmpeg")?;
        if !status.success() {
            return Err(anyhow!("ffmpeg a échoué (code {:?})", status.code()));
        }
        Ok(())
    }
}

impl Drop for FfmpegHandle {
    fn drop(&mut self) {
        // Kill immédiat si le handle est drop avant la fin normale
        // (déconnexion client, timeout, erreur axum...)
        let _ = self.child.start_kill();
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Échappe les caractères spéciaux pour le filtre drawtext de FFmpeg.
///
/// drawtext interprète : ' \ : comme des délimiteurs.
/// On échappe avec \ pour les rendre littéraux.
fn escape_drawtext(s: &str) -> String {
    s.chars().flat_map(|c| match c {
        '\'' => vec!['\\', '\''],
        '\\' => vec!['\\', '\\'],
        ':'  => vec!['\\', ':'],
        c    => vec![c],
    }).collect()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_special_chars() {
        assert_eq!(escape_drawtext("Jean-Pierre"),     "Jean-Pierre");
        assert_eq!(escape_drawtext("O'Brien"),         "O\\'Brien");
        assert_eq!(escape_drawtext("test:value"),      "test\\:value");
        assert_eq!(escape_drawtext("back\\slash"),     "back\\\\slash");
    }
}