# metrowrap

Metrolist'in **Listen Together** odasını PC'de dinlemenizi sağlayan wrapper.  
Arka planda çalışır, sistem tray'inde ikon gösterir, `localhost:7823` adresinde web arayüzü sunar.

---

## Kurulum

### Gereksinimler

**Python 3.11+**

**mpv** — https://mpv.io/installation/  
Windows: `winget install mpv.net` veya resmi siteden indirin. PATH'e ekleyin.

**yt-dlp** (mpv'nin YouTube akışı çekebilmesi için):

    pip install yt-dlp

### Python paketleri

`listentogether_pb2.py` zaten derlenmiş olarak geliyor; tekrar derlemeye gerek yok.

    pip install websockets protobuf fastapi uvicorn pystray Pillow yt-dlp

---

## Kullanım

    python metrowrap.py

Başladığında:

- Sistem tray'e ikon eklenir (yeşil = bağlı, gri = bağlı değil)
- Tarayıcıda `http://localhost:7823` otomatik açılır

### Oda bağlantısı

1. Mobil Metrolist'te **Listen Together** odasını açın
2. Web arayüzünde oda kodunu, kullanıcı adınızı ve sunucu URL'sini girin
3. **Bağlan**'a tıklayın
4. Mobilde gelen isteği onaylayın
5. Şarkılar mpv'de otomatik çalmaya başlar

### CLI seçenekleri

    --port    PORT    Web UI portu (varsayılan: 7823)
    --server  URL     Varsayılan sunucu (UI'dan da değiştirilebilir)
    --no-tray         Tray ikonu olmadan çalıştır

---

## Sunucu URL'si

`wss://metroserver.nyxie.dev/ws` bilinen public instance'dır.  
Değişmişse `--server` ile veya UI'daki Sunucu alanından geçersiz kılabilirsiniz.

Gerçek URL'yi bulmak için APK'yı `apktool` ile açıp  
`com/metrolist/music/listentogether/` içinde aratın.

---

## Mimari

    [Mobil Metrolist]
           | WebSocket (protobuf + gzip)
           v
     [metroserver]
           |
           | sync_playback / sync_state
           v
     [metrowrap.py] ----join_room----> [metroserver]
       |         |
       |         +-- FastAPI --> http://localhost:7823
       |                          (Web UI + SSE)
       v
     [mpv] <-- yt-dlp --> YouTube stream
       |
       +-- JSON IPC (named pipe / unix socket)

---

## Dosyalar

    metrowrap.py           Ana uygulama (tek dosya)
    listentogether_pb2.py  Derlenmiş protobuf (hazır gelir)
    listentogether.proto   Proto semasi (referans)
    requirements.txt       Python bagimliliklar

---

## Bilinen sınırlamalar

- Sadece **dinleyici** rolü desteklenir, host olunamaz.
- Zaman senkronizasyonu yaklaşık; yüksek gecikmede ±2s kayma olabilir.
- Linux'ta sistem tray için GTK ortamı gerekir; headless ortamda `--no-tray` kullanın.
- yt-dlp YouTube Music'e erişebilmeli; bölgenizde kısıtlıysa VPN kullanın.
