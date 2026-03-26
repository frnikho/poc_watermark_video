RUST_LOG=debug cargo run

# Créer une session (la vidéo doit avoir ses metadata dans Valkey)
curl -X POST http://localhost:3000/session \
  -H "Content-Type: application/json" \
  -d '{"video_key":"ma-video.mp4","viewer_id":"user_42","firstname":"Jean","lastname":"Dupont"}'

# Récupérer le M3U8
curl http://localhost:3000/session/XXXXXXXXXXXXXXXX/m3u8