ffmpeg -i gims.mp4 -vf "split=5[out1][p1][p2][p3][p4]; \
[p1]crop=20:20:iw*0.1:ih*0.1,eq=brightness=0.10[p1w]; \
[p2]crop=20:20:iw*0.9:ih*0.1,eq=brightness=0.10[p2w]; \
[p3]crop=20:20:iw*0.1:ih*0.9,eq=brightness=0.10[p3w]; \
[p4]crop=20:20:iw*0.9:ih*0.9,eq=brightness=0.10[p4w]; \
[out1][p1w]overlay=x=W*0.1:y=H*0.1[v2]; \
[v2][p2w]overlay=x=W*0.9:y=H*0.1[v3]; \
[v3][p3w]overlay=x=W*0.1:y=H*0.9[v4]; \
[v4][p4w]overlay=x=W*0.9:y=H*0.9" \
-c:v libx264  -c:a copy video_B.mp4




curl -X POST http://localhost:3000/metadata \
-H "Content-Type: application/json" \
-d '{
"video_key":   "gims.mp4",
"presign_url": "https://pub-6648107f7aaa4abb97da7dea6586317c.r2.dev/GIMS%20%26%20La%20Mano%201.9%20-%20PARISIENNE%20(Clip%20officiel)%20%5B7CGKeID7nRc%5D.mp4"
}'