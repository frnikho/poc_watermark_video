use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Semaphore, mpsc};
use std::process::Stdio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Démarrage du pipeline...");

    let presigned_url = "https://pub-6648107f7aaa4abb97da7dea6586317c.r2.dev/output_big.mp4";
    let presigned_url = "https://pub-6648107f7aaa4abb97da7dea6586317c.r2.dev/GIMS%20%26%20La%20Mano%201.9%20-%20PARISIENNE%20(Clip%20officiel)%20%5B7CGKeID7nRc%5D.mp4";
    let r2_endpoint = "https://42f7cb419c69ad517152e928172c5b21.r2.cloudflarestorage.com";
    let bucket_out = "test-watermark";
    let key_out = "video-processed.mp4";

    let region_provider = RegionProviderChain::default_provider().or_else("auto");
    let config = aws_config::from_env()
        .region(region_provider)
        .endpoint_url(r2_endpoint)
        .load()
        .await;
    let s3_client = Arc::new(Client::new(&config));

    let filter_spec = "[1:v]scale=80:109[logo];[0:v][logo]overlay=10:main_h-overlay_h-10";

    let mut child = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-i").arg(presigned_url)
        .arg("-i").arg("watermark.png")
        .arg("-filter_complex").arg(filter_spec)
        .arg("-c:v").arg("libx264")
        .arg("-preset").arg("veryfast")
        .arg("-f").arg("mp4")
        .arg("-movflags").arg("frag_keyframe+empty_moov")
        .arg("-progress").arg("pipe:2")
        .arg("pipe:1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // Channel pour remonter les erreurs FFmpeg détectées dans stderr
    let (ffmpeg_err_tx, mut ffmpeg_err_rx) = mpsc::channel::<String>(1);

    let monitoring_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut last_error = String::new();

        while let Ok(Some(line)) = reader.next_line().await {
            if line.starts_with("out_time=") {
                println!("⏱️  {}", line.replace("out_time=", ""));
            }
            // FFmpeg écrit ses erreurs sur stderr aussi
            if line.contains("Error") || line.contains("error") || line.contains("Invalid") {
                last_error = line.clone();
                eprintln!("⚠️  FFmpeg stderr: {}", line);
            }
        }

        if !last_error.is_empty() {
            let _ = ffmpeg_err_tx.send(last_error).await;
        }
    });

    // --- Multipart upload avec abort sur erreur ---
    let result = run_upload(&s3_client, bucket_out, key_out, &mut stdout).await;

    // On attend la fin de FFmpeg dans tous les cas
    let ffmpeg_status = child.wait().await?;
    let _ = monitoring_handle.await;

    // Vérification du exit code FFmpeg
    if !ffmpeg_status.success() {
        let ffmpeg_msg = ffmpeg_err_rx.try_recv().unwrap_or_else(|_| "inconnu".to_string());
        return Err(format!(
            "FFmpeg a échoué (code {:?}) : {}",
            ffmpeg_status.code(),
            ffmpeg_msg
        ).into());
    }

    result?; // Propage l'erreur R2 si elle existe

    println!("🎉 Job terminé avec succès !");
    Ok(())
}

async fn run_upload(
    s3_client: &Arc<Client>,
    bucket: &str,
    key: &str,
    stdout: &mut tokio::process::ChildStdout,
) -> Result<(), Box<dyn std::error::Error>> {
    let multipart = s3_client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart.upload_id()
        .ok_or("Pas d'upload_id retourné par R2")?
        .to_string();

    // On wrappe toute la logique pour pouvoir abort proprement en cas d'erreur
    match upload_parts(s3_client, bucket, key, &upload_id, stdout).await {
        Ok(completed_parts) => {
            let final_upload = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            s3_client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(final_upload)
                .send()
                .await?;

            println!("🏁 Upload terminé avec succès sur R2 !");
            Ok(())
        }
        Err(e) => {
            // CRITICAL : abort pour ne pas laisser des parts orphelines sur R2
            eprintln!("❌ Erreur upload, annulation du multipart ({})...", upload_id);
            if let Err(abort_err) = s3_client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await
            {
                eprintln!("⚠️  Échec de l'abort multipart : {}", abort_err);
                // Log mais on remonte l'erreur originale
            } else {
                println!("🧹 Multipart upload annulé proprement.");
            }
            Err(e)
        }
    }
}

async fn upload_parts(
    s3_client: &Arc<Client>,
    bucket: &str,
    key: &str,
    upload_id: &str,
    stdout: &mut tokio::process::ChildStdout,
) -> Result<Vec<CompletedPart>, Box<dyn std::error::Error>> {
    let semaphore = Arc::new(Semaphore::new(3));
    let chunk_size = 15 * 1024 * 1024;
    let mut part_number = 1i32;
    let mut upload_tasks = Vec::new();

    loop {
        let mut buffer = vec![0u8; chunk_size];
        let mut bytes_read = 0;

        while bytes_read < chunk_size {
            let n = stdout.read(&mut buffer[bytes_read..]).await?;
            if n == 0 { break; }
            bytes_read += n;
        }

        if bytes_read == 0 { break; }
        buffer.truncate(bytes_read);

        let s3_clone = Arc::clone(s3_client);
        let sem_clone = Arc::clone(&semaphore);
        let uid = upload_id.to_string();
        let bkt = bucket.to_string();
        let ky = key.to_string();
        let p_num = part_number;

        let task = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            let body = aws_sdk_s3::primitives::ByteStream::from(buffer);

            let resp = s3_clone
                .upload_part()
                .bucket(bkt)
                .key(ky)
                .upload_id(uid)
                .part_number(p_num)
                .body(body)
                .send()
                .await?; // <- propagation propre

            let etag = resp.e_tag()
                .ok_or("ETag manquant dans la réponse R2")?
                .to_string();

            Ok::<(i32, String), Box<dyn std::error::Error + Send + Sync>>((p_num, etag))
        });

        upload_tasks.push(task);
        part_number += 1;
    }

    if upload_tasks.is_empty() {
        return Err("FFmpeg n'a produit aucune donnée".into());
    }

    let mut completed_parts = Vec::new();
    for task in upload_tasks {
        let (p_num, etag) = (task.await?).unwrap(); // double ? : JoinError + notre erreur
        completed_parts.push(
            CompletedPart::builder()
                .e_tag(etag)
                .part_number(p_num)
                .build()
        );
    }

    Ok(completed_parts)
}