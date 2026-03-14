use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Semaphore;
use std::process::Stdio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Démarrage du pipeline optimisé (Lecture directe via FFmpeg)...");

    let presigned_url = "https://pub-6648107f7aaa4abb97da7dea6586317c.r2.dev/output_big.mp4";
    let presigned_url = "https://pub-6648107f7aaa4abb97da7dea6586317c.r2.dev/GIMS%20%26%20La%20Mano%201.9%20-%20PARISIENNE%20(Clip%20officiel)%20%5B7CGKeID7nRc%5D.mp4";
    let r2_endpoint = "https://42f7cb419c69ad517152e928172c5b21.r2.cloudflarestorage.com";
    let bucket_out = "test-watermark";
    let key_out = "video-processed.mp4";

    // Initialisation S3
    let region_provider = RegionProviderChain::default_provider().or_else("auto");
    let config = aws_config::from_env()
        .region(region_provider)
        .endpoint_url(r2_endpoint)
        .load()
        .await;
    let s3_client = Arc::new(Client::new(&config));

    let filter_spec = "[1:v]scale=80:109[logo];[0:v][logo]overlay=10:main_h-overlay_h-10";

    // FFMPEG
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

    let monitoring_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if line.starts_with("out_time=") {
                println!("⏱️ FFmpeg Progress: {}", line.replace("out_time=", ""));
            }
        }
    });

    let s3_c = Arc::clone(&s3_client);
    let writer_handle = tokio::spawn(async move {
        let multipart = s3_c.create_multipart_upload()
            .bucket(bucket_out).key(key_out).send().await.expect("Erreur init multipart");
        let upload_id = multipart.upload_id().unwrap().to_string();

        let mut upload_tasks = Vec::new();
        let semaphore = Arc::new(Semaphore::new(3));
        let chunk_size = 15 * 1024 * 1024;
        let mut part_number = 1;

        loop {
            let mut buffer = vec![0u8; chunk_size];
            let mut bytes_read = 0;

            while bytes_read < chunk_size {
                let n = stdout.read(&mut buffer[bytes_read..]).await.expect("Erreur lecture stdout");
                if n == 0 { break; }
                bytes_read += n;
            }

            if bytes_read == 0 { break; }
            buffer.truncate(bytes_read);

            let s3_clone = Arc::clone(&s3_c);
            let sem_clone = Arc::clone(&semaphore);
            let uid = upload_id.clone();
            let bkt = bucket_out.to_string();
            let ky = key_out.to_string();
            let p_num = part_number;

            let task = tokio::spawn(async move {
                let _permit = sem_clone.acquire().await.unwrap();
                let body = aws_sdk_s3::primitives::ByteStream::from(buffer);

                let resp = s3_clone.upload_part()
                    .bucket(bkt).key(ky).upload_id(uid).part_number(p_num)
                    .body(body).send().await.expect("Erreur upload part");

                (p_num, resp.e_tag().unwrap().to_string())
            });

            upload_tasks.push(task);
            part_number += 1;
        }

        if upload_tasks.is_empty() {
            println!("⚠️ Aucune donnée produite par FFmpeg.");
            return;
        }

        let mut completed_parts = Vec::new();
        for task in upload_tasks {
            let (p_num, etag) = task.await.unwrap();
            completed_parts.push(CompletedPart::builder()
                .e_tag(etag).part_number(p_num).build());
        }

        let final_upload = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();
        s3_c.complete_multipart_upload()
            .bucket(bucket_out).key(key_out).upload_id(upload_id).multipart_upload(final_upload)
            .send().await.expect("Erreur finalisation");

        println!("🏁 Upload terminé avec succès sur R2 !");
    });

    let _ = tokio::join!(monitoring_handle, writer_handle);
    let status = child.wait().await?;

    println!("🎉 Job terminé avec succès. Status : {}", status);

    Ok(())
}