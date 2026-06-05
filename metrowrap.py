"""
metrowrap.py — Metrolist Listen Together PC Wrapper
Tray + Web UI (localhost:7823) — Tarayıcı tabanlı ses oynatma

Bagimliliklar:
    pip install websockets protobuf fastapi uvicorn pystray Pillow yt-dlp

Kullanim:
    python metrowrap.py
    python metrowrap.py --port 7823 --server wss://metroserver.nyxie.dev/ws
    python metrowrap.py --no-tray
    python metrowrap.py --cache-dir /tmp/metrowrap_cache
"""

import asyncio
import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

# ── Protobuf ──────────────────────────────────────────────────────────────────
try:
    import listentogether_pb2 as pb
except ImportError:
    print("[HATA] listentogether_pb2.py bulunamadi.")
    sys.exit(1)

# ── Dis kutuphaneler ──────────────────────────────────────────────────────────
try:
    import websockets
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    from PIL import Image, ImageDraw
except ImportError as e:
    print(f"[HATA] Eksik kutuphane: {e}")
    print("Cozum: pip install websockets protobuf fastapi uvicorn pystray Pillow yt-dlp")
    sys.exit(1)

_TRAY_AVAILABLE = False
try:
    import pystray
    _TRAY_AVAILABLE = True
except Exception:
    pass

# ── Sabitler ──────────────────────────────────────────────────────────────────

DEFAULT_SERVER    = "wss://metroserverx.meowery.eu/ws"
DEFAULT_PORT      = 7823
DEFAULT_CACHE_DIR = Path(tempfile.gettempdir()) / "metrowrap_cache"

MSG_JOIN_ROOM         = "join_room"
MSG_LEAVE_ROOM        = "leave_room"
MSG_BUFFER_READY      = "buffer_ready"
MSG_REQUEST_SYNC      = "request_sync"
MSG_PING              = "ping"
MSG_JOIN_APPROVED     = "join_approved"
MSG_JOIN_REJECTED     = "join_rejected"
MSG_SYNC_PLAYBACK     = "sync_playback"
MSG_SYNC_STATE        = "sync_state"
MSG_BUFFER_COMPLETE   = "buffer_complete"
MSG_BUFFER_WAIT       = "buffer_wait"
MSG_USER_JOINED       = "user_joined"
MSG_USER_LEFT         = "user_left"
MSG_USER_DISCONNECTED = "user_disconnected"
MSG_USER_RECONNECTED  = "user_reconnected"
MSG_HOST_CHANGED      = "host_changed"
MSG_KICKED            = "kicked"
MSG_PONG              = "pong"
MSG_ERROR             = "error"

ACTION_PLAY         = "play"
ACTION_PAUSE        = "pause"
ACTION_SEEK         = "seek"
ACTION_CHANGE_TRACK = "change_track"
ACTION_SET_VOLUME   = "set_volume"
ACTION_QUEUE_ADD    = "queue_add"
ACTION_QUEUE_REMOVE = "queue_remove"
ACTION_QUEUE_CLEAR  = "queue_clear"
ACTION_SYNC_QUEUE   = "sync_queue"

# ── Cache ─────────────────────────────────────────────────────────────────────

class TrackCache:
    """
    Disk cache: track_id → .webm/.opus dosyası
    İndirme tamamlanana kadar .part uzantısıyla tutulur.
    """

    def __init__(self, cache_dir: Path):
        self.dir = cache_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        # track_id → {"status": "downloading"|"ready"|"error", "path": Path, "url": str}
        self._meta: dict = {}
        self._lock = threading.Lock()

    def _part_path(self, track_id: str) -> Path:
        return self.dir / f"{track_id}.part"

    def _done_path(self, track_id: str) -> Path:
        # yt-dlp hangi uzantıyı seçerse seçsin .webm dönüştürüyoruz
        return self.dir / f"{track_id}.webm"

    def status(self, track_id: str) -> str:
        """ready | downloading | error | missing"""
        with self._lock:
            if track_id in self._meta:
                return self._meta[track_id]["status"]
        # Disk'te tamamlanmış dosya var mı?
        if self._done_path(track_id).exists():
            with self._lock:
                self._meta[track_id] = {
                    "status": "ready",
                    "path": self._done_path(track_id),
                    "url": "",
                }
            return "ready"
        return "missing"

    def get_path(self, track_id: str) -> Optional[Path]:
        with self._lock:
            m = self._meta.get(track_id)
            if m and m["status"] == "ready":
                return m["path"]
        p = self._done_path(track_id)
        return p if p.exists() else None

    def start_download(self, track_id: str):
        """Arka planda yt-dlp ile indir."""
        with self._lock:
            if track_id in self._meta:
                return  # zaten başladı/tamamlandı
            self._meta[track_id] = {"status": "downloading", "path": None, "url": ""}

        def _worker():
            out = str(self._done_path(track_id))
            tmp = str(self._part_path(track_id))
            cmd = [
                "yt-dlp",
                "--no-playlist",
                "-x",                          # sadece ses
                "--audio-format", "webm",
                "--audio-quality", "0",
                "-o", tmp,
                "--no-part",                   # .part yerine doğrudan yaz
                f"https://music.youtube.com/watch?v={track_id}",
            ]
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=120,
                )
                if result.returncode == 0 and Path(tmp).exists():
                    Path(tmp).rename(out)
                    with self._lock:
                        self._meta[track_id] = {
                            "status": "ready",
                            "path": Path(out),
                            "url": "",
                        }
                    state.add_log(f"Cache hazir: {track_id[:8]}…")
                else:
                    err = result.stderr.decode(errors="replace")[-200:]
                    state.add_log(f"Cache indirme hatasi ({track_id[:8]}): {err}", "warn")
                    with self._lock:
                        self._meta[track_id]["status"] = "error"
            except Exception as e:
                state.add_log(f"Cache worker exception: {e}", "warn")
                with self._lock:
                    self._meta[track_id]["status"] = "error"

        threading.Thread(target=_worker, daemon=True).start()

    def get_stream_url(self, track_id: str) -> str:
        """yt-dlp ile anlık audio URL al (redirect için)."""
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "-x",
            "--get-url",
            f"https://music.youtube.com/watch?v={track_id}",
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                url = result.stdout.strip().splitlines()[0]
                return url
        except Exception as e:
            state.add_log(f"yt-dlp URL hatasi: {e}", "error")
        return ""

    def clear_old(self, keep_ids: list, max_files: int = 20):
        """Cache klasöründe en fazla max_files dosya tut."""
        files = sorted(
            self.dir.glob("*.webm"),
            key=lambda p: p.stat().st_mtime
        )
        to_delete = [f for f in files if f.stem not in keep_ids]
        while len(files) - len(to_delete) > max_files and to_delete:
            f = to_delete.pop(0)
            try:
                f.unlink()
            except Exception:
                pass


cache: TrackCache  # init'de atanır

# ── Uygulama durumu ───────────────────────────────────────────────────────────

class AppState:
    def __init__(self):
        self._lock = threading.Lock()
        self.status      = "idle"
        self.status_msg  = "Bagli degil"
        self.room_code   = ""
        self.username    = ""
        self.server_url  = DEFAULT_SERVER
        self.user_id     = ""
        self.is_playing  = False
        self.position_ms = 0
        self.position_ts = 0.0
        self.volume      = 1.0
        self.current_track  = None
        self.users       = []
        self.queue       = []
        self.logs        = []
        self._version    = 0
        # Tarayıcıya gönderilecek komut kuyruğu (SSE)
        self._cmd_queue: list = []

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)
            self._version += 1

    def push_cmd(self, cmd: dict):
        """Tarayıcıya anlık komut gönder (SSE üzerinden)."""
        with self._lock:
            self._cmd_queue.append(cmd)
            self._version += 1

    def pop_cmds(self) -> list:
        with self._lock:
            cmds = list(self._cmd_queue)
            self._cmd_queue.clear()
            return cmds

    def add_log(self, msg: str, level: str = "info"):
        entry = {"t": time.strftime("%H:%M:%S"), "msg": msg, "level": level}
        with self._lock:
            self.logs.append(entry)
            if len(self.logs) > 200:
                self.logs = self.logs[-150:]
            self._version += 1
        print(f"[{entry['t']}] {msg}")

    def snapshot(self) -> dict:
        with self._lock:
            pos = self.position_ms
            if self.is_playing and self.position_ts > 0:
                pos += int((time.monotonic() - self.position_ts) * 1000)
            cmds = list(self._cmd_queue)
            return {
                "status":        self.status,
                "status_msg":    self.status_msg,
                "room_code":     self.room_code,
                "username":      self.username,
                "server_url":    self.server_url,
                "is_playing":    self.is_playing,
                "position_ms":   max(0, pos),
                "volume":        self.volume,
                "current_track": self.current_track,
                "users":         list(self.users),
                "queue":         list(self.queue),
                "logs":          self.logs[-60:],
                "version":       self._version,
                "cmds":          cmds,
            }

    @property
    def version(self):
        with self._lock:
            return self._version


state = AppState()
state.add_log("metrowrap baslatildi")

# ── Codec ─────────────────────────────────────────────────────────────────────

def encode_msg(msg_type: str, proto_obj=None) -> bytes:
    raw = proto_obj.SerializeToString() if proto_obj else b""
    compressed = False
    if len(raw) > 100:
        c = gzip.compress(raw)
        if len(c) < len(raw):
            raw, compressed = c, True
    env = pb.Envelope(type=msg_type, payload=raw, compressed=compressed)
    return env.SerializeToString()


def decode_msg(data: bytes):
    env = pb.Envelope()
    env.ParseFromString(data)
    payload = env.payload
    if env.compressed and payload:
        payload = gzip.decompress(payload)
    return env.type, payload


_PAYLOAD_MAP = {
    MSG_JOIN_APPROVED:     pb.JoinApprovedPayload,
    MSG_JOIN_REJECTED:     pb.JoinRejectedPayload,
    MSG_SYNC_PLAYBACK:     pb.PlaybackActionPayload,
    MSG_SYNC_STATE:        pb.SyncStatePayload,
    MSG_BUFFER_COMPLETE:   pb.BufferCompletePayload,
    MSG_BUFFER_WAIT:       pb.BufferWaitPayload,
    MSG_USER_JOINED:       pb.UserJoinedPayload,
    MSG_USER_LEFT:         pb.UserLeftPayload,
    MSG_USER_DISCONNECTED: pb.UserDisconnectedPayload,
    MSG_USER_RECONNECTED:  pb.UserReconnectedPayload,
    MSG_HOST_CHANGED:      pb.HostChangedPayload,
    MSG_KICKED:            pb.KickedPayload,
    MSG_ERROR:             pb.ErrorPayload,
}


def parse_payload(msg_type: str, data: bytes):
    klass = _PAYLOAD_MAP.get(msg_type)
    if not klass:
        return None
    obj = klass()
    if data:
        obj.ParseFromString(data)
    return obj


# ── WebSocket istemcisi ───────────────────────────────────────────────────────

class MetroClient:
    def __init__(self):
        self._ws = None
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._track_id = ""

    async def connect(self, server_url: str, room_code: str, username: str):
        if self._task and not self._task.done():
            await self._disconnect_ws()
        state.update(
            status="connecting",
            status_msg=f"Baglaniliyor: {server_url}",
            room_code=room_code,
            username=username,
            server_url=server_url,
        )
        self._task = asyncio.create_task(
            self._run(server_url, room_code, username)
        )

    async def disconnect(self):
        await self._disconnect_ws()
        self._track_id = ""
        # Tarayıcıya durdur komutu
        state.push_cmd({"op": "pause"})
        state.update(
            status="idle", status_msg="Bagli degil",
            current_track=None, is_playing=False,
            users=[], queue=[],
        )
        state.add_log("Baganti kesildi")

    async def _disconnect_ws(self):
        if self._ws:
            try:
                await self._ws.send(encode_msg(MSG_LEAVE_ROOM))
                await self._ws.close()
            except:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except:
                pass
        self._ws = None

    async def _run(self, server_url: str, room_code: str, username: str):
        try:
            async with websockets.connect(
                server_url, ping_interval=None, close_timeout=5
            ) as ws:
                self._ws = ws
                state.add_log("Sunucuya baglandi")

                await ws.send(encode_msg(
                    MSG_JOIN_ROOM,
                    pb.JoinRoomPayload(room_code=room_code, username=username),
                ))
                state.update(status="waiting", status_msg="Host onayi bekleniyor...")
                state.add_log(f"Oda {room_code} icin istek gonderildi")

                self._ping_task = asyncio.create_task(self._ping_loop())

                async for raw in ws:
                    if isinstance(raw, bytes):
                        await self._handle(raw)

        except websockets.exceptions.ConnectionClosedError as e:
            state.add_log(f"Baglanti kapandi: {e}", "error")
        except OSError as e:
            state.add_log(f"Baglanti hatasi: {e}", "error")
        except asyncio.CancelledError:
            pass
        finally:
            if self._ping_task:
                self._ping_task.cancel()
            self._ws = None
            if state.status not in ("idle",):
                state.update(status="error", status_msg="Baglanti kesildi")

    async def _ping_loop(self):
        while True:
            await asyncio.sleep(30)
            try:
                await self._ws.send(encode_msg(MSG_PING))
            except:
                break

    async def _handle(self, raw: bytes):
        try:
            msg_type, payload_bytes = decode_msg(raw)
        except Exception as e:
            state.add_log(f"Decode hatasi: {e}", "warn")
            return

        obj = parse_payload(msg_type, payload_bytes)

        if msg_type == MSG_JOIN_APPROVED:
            await self._on_joined(obj)
        elif msg_type == MSG_JOIN_REJECTED:
            state.add_log(f"Giris reddedildi: {obj.reason}", "error")
            state.update(status="error", status_msg=f"Reddedildi: {obj.reason}")
        elif msg_type == MSG_KICKED:
            state.add_log(f"Odadan atildiniz: {obj.reason}", "error")
            state.update(status="idle", status_msg="Odadan atildiniz")
            state.push_cmd({"op": "pause"})
        elif msg_type == MSG_HOST_CHANGED:
            state.add_log(f"Yeni host: {obj.new_host_name}")
        elif msg_type == MSG_USER_JOINED:
            state.add_log(f"+ {obj.username} katildi")
            with state._lock:
                state.users = [u for u in state.users if u["id"] != obj.user_id]
                state.users.append({
                    "id": obj.user_id, "name": obj.username,
                    "is_host": False, "connected": True,
                })
                state._version += 1
        elif msg_type == MSG_USER_LEFT:
            state.add_log(f"- {obj.username} ayrildi")
            with state._lock:
                state.users = [u for u in state.users if u["id"] != obj.user_id]
                state._version += 1
        elif msg_type in (MSG_USER_DISCONNECTED, MSG_USER_RECONNECTED):
            connected = msg_type == MSG_USER_RECONNECTED
            with state._lock:
                for u in state.users:
                    if u["id"] == obj.user_id:
                        u["connected"] = connected
                state._version += 1
        elif msg_type == MSG_SYNC_STATE:
            await self._apply_sync_state(obj)
        elif msg_type == MSG_SYNC_PLAYBACK:
            await self._apply_action(obj)
        elif msg_type == MSG_ERROR:
            state.add_log(f"[{obj.code}] {obj.message}", "error")

    async def _on_joined(self, obj):
        state.update(
            user_id=obj.user_id,
            status="connected",
            status_msg=f"Baglandi · {state.room_code}",
        )
        state.add_log("Odaya girildi!")

        s = obj.state
        users = [
            {
                "id": u.user_id, "name": u.username,
                "is_host": u.is_host, "connected": u.is_connected,
            }
            for u in s.users
        ]
        state.update(users=users)

        try:
            has_track = s.HasField("current_track")
        except ValueError:
            has_track = bool(s.current_track.id)

        if has_track and s.current_track.id:
            await self._load_track(s.current_track, s.position, s.is_playing)

        await self._ws.send(encode_msg(MSG_REQUEST_SYNC))

    async def _apply_sync_state(self, obj):
        try:
            has_track = obj.HasField("current_track")
        except ValueError:
            has_track = bool(obj.current_track.id)

        if not has_track:
            return

        t   = obj.current_track
        pos = _live_pos(obj.position, obj.last_update, obj.is_playing)

        if t.id != self._track_id:
            await self._load_track(t, pos, obj.is_playing)
        else:
            if obj.is_playing:
                state.push_cmd({"op": "play", "pos_ms": pos})
            else:
                state.push_cmd({"op": "pause", "pos_ms": pos})
            state.update(
                is_playing=obj.is_playing, position_ms=pos,
                position_ts=time.monotonic() if obj.is_playing else 0,
            )

        state.push_cmd({"op": "volume", "v": obj.volume})
        state.update(volume=obj.volume)

        queue = [
            {"id": q.id, "title": q.title, "artist": q.artist, "thumbnail": q.thumbnail}
            for q in obj.queue
        ]
        state.update(queue=queue)

    async def _apply_action(self, obj):
        action = obj.action

        if action == ACTION_CHANGE_TRACK:
            if not obj.track_info.id:
                return
            t = obj.track_info
            state.add_log(f"Sarki: {t.title} - {t.artist}")
            await self._load_track(t, 0, False)
        elif action == ACTION_PLAY:
            pos = _live_pos(obj.position, obj.server_time, True)
            state.push_cmd({"op": "play", "pos_ms": pos})
            state.update(is_playing=True, position_ms=pos, position_ts=time.monotonic())
            state.add_log("Oynatiliyor")
        elif action == ACTION_PAUSE:
            state.push_cmd({"op": "pause", "pos_ms": obj.position})
            state.update(is_playing=False, position_ms=obj.position, position_ts=0)
            state.add_log("Duraklatildi")
        elif action == ACTION_SEEK:
            state.push_cmd({"op": "seek", "pos_ms": obj.position})
            state.update(position_ms=obj.position, position_ts=time.monotonic())
        elif action == ACTION_SET_VOLUME:
            state.push_cmd({"op": "volume", "v": obj.volume})
            state.update(volume=obj.volume)

    async def _load_track(self, proto_track, pos_ms: int, play: bool):
        self._track_id = proto_track.id
        track = {
            "id":          proto_track.id,
            "title":       proto_track.title,
            "artist":      proto_track.artist,
            "album":       proto_track.album,
            "duration_ms": proto_track.duration,
            "thumbnail":   proto_track.thumbnail,
        }
        state.update(
            current_track=track,
            is_playing=play,
            position_ms=pos_ms,
            position_ts=time.monotonic() if play else 0,
        )

        tid = proto_track.id
        cache_status = cache.status(tid)

        if cache_status == "missing":
            # Arka planda indirmeye başla
            cache.start_download(tid)
            state.add_log(f"Sarki indiriliyor: {proto_track.title[:30]}")

        # Tarayıcıya yeni track komutu: stream URL'si /api/stream/{id}
        state.push_cmd({
            "op":    "load",
            "src":   f"/api/stream/{tid}",
            "pos_ms": pos_ms,
            "play":  play,
            "track": track,
        })

        # buffer_ready hemen gönder — tarayıcı kendi bufferını yönetecek
        if self._ws:
            try:
                await self._ws.send(encode_msg(
                    MSG_BUFFER_READY,
                    pb.BufferReadyPayload(track_id=tid),
                ))
            except:
                pass


def _live_pos(position_ms: int, last_update_ms: int, is_playing: bool) -> int:
    if not is_playing or not last_update_ms:
        return position_ms
    now_ms = int(time.time() * 1000)
    return max(0, position_ms + (now_ms - last_update_ms))


# ── Stream endpoint yardımcıları ──────────────────────────────────────────────

def _parse_range(range_header: str, file_size: int):
    """Range: bytes=START-END → (start, end)"""
    try:
        unit, rng = range_header.split("=", 1)
        if unit.strip() != "bytes":
            return 0, file_size - 1
        start_s, end_s = rng.strip().split("-", 1)
        start = int(start_s) if start_s else 0
        end   = int(end_s)   if end_s   else file_size - 1
        end   = min(end, file_size - 1)
        return start, end
    except Exception:
        return 0, file_size - 1


async def _stream_file(path: Path, request: Request):
    """Disk'teki dosyayı Range destekli şekilde stream et."""
    file_size = path.stat().st_size
    range_header = request.headers.get("range")

    if range_header:
        start, end = _parse_range(range_header, file_size)
        length = end - start + 1

        async def gen_partial():
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                chunk = 65536
                while remaining > 0:
                    data = f.read(min(chunk, remaining))
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return StreamingResponse(
            gen_partial(),
            status_code=206,
            media_type="audio/webm",
            headers={
                "Content-Range":  f"bytes {start}-{end}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges":  "bytes",
                "Cache-Control":  "no-cache",
            },
        )
    else:
        async def gen_full():
            with open(path, "rb") as f:
                while True:
                    data = f.read(65536)
                    if not data:
                        break
                    yield data

        return StreamingResponse(
            gen_full(),
            status_code=200,
            media_type="audio/webm",
            headers={
                "Content-Length": str(file_size),
                "Accept-Ranges":  "bytes",
                "Cache-Control":  "no-cache",
            },
        )


async def _proxy_yt_stream(url: str, request: Request):
    """
    YouTube CDN URL'sini proxy et — httpx ile byte chunk'ları tarayıcıya ilet.
    Range header'ı olduğu gibi upstream'e geçirir.
    """
    try:
        import httpx
    except ImportError:
        # httpx yoksa yt-dlp'yi subprocess ile çağırıp stdout'u pipe et
        return await _ytdlp_pipe_stream(request, url)

    range_header = request.headers.get("range")
    headers = {}
    if range_header:
        headers["Range"] = range_header

    async def gen():
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            async with client.stream("GET", url, headers=headers) as resp:
                async for chunk in resp.aiter_bytes(65536):
                    yield chunk

    # upstream status kodu ve Content-* headerlarını al
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        head = await client.head(url, headers=headers)

    status  = 206 if range_header else 200
    resp_h  = {}
    for k in ("Content-Length", "Content-Range", "Content-Type", "Accept-Ranges"):
        if k in head.headers:
            resp_h[k] = head.headers[k]
    resp_h.setdefault("Accept-Ranges", "bytes")
    resp_h.setdefault("Cache-Control", "no-cache")

    return StreamingResponse(
        gen(),
        status_code=status,
        media_type=resp_h.get("Content-Type", "audio/webm"),
        headers={k: v for k, v in resp_h.items() if k != "Content-Type"},
    )


async def _ytdlp_pipe_stream(request: Request, direct_url: str = ""):
    """httpx olmadığında: doğrudan URL'yi urllib ile pipe et."""
    import urllib.request

    range_header = request.headers.get("range")
    req = urllib.request.Request(direct_url or "")
    if range_header:
        req.add_header("Range", range_header)
    req.add_header("User-Agent", "Mozilla/5.0")

    def _open():
        return urllib.request.urlopen(req, timeout=30)

    loop = asyncio.get_event_loop()
    resp_obj = await loop.run_in_executor(None, _open)

    status = resp_obj.status
    ct     = resp_obj.headers.get("Content-Type", "audio/webm")
    cl     = resp_obj.headers.get("Content-Length", "")
    cr     = resp_obj.headers.get("Content-Range", "")

    hdrs = {"Accept-Ranges": "bytes", "Cache-Control": "no-cache"}
    if cl: hdrs["Content-Length"] = cl
    if cr: hdrs["Content-Range"] = cr

    async def gen():
        while True:
            data = await loop.run_in_executor(None, resp_obj.read, 65536)
            if not data:
                break
            yield data

    return StreamingResponse(gen(), status_code=status, media_type=ct, headers=hdrs)


# ── Tray ─────────────────────────────────────────────────────────────────────

def _make_icon(connected: bool) -> Image.Image:
    size = 64
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    color = (127, 187, 179) if connected else (86, 90, 112)
    d.ellipse([4, 4, size - 4, size - 4], fill=color)
    nota = (15, 17, 23)
    d.ellipse([16, 36, 27, 47], fill=nota)
    d.ellipse([36, 40, 47, 51], fill=nota)
    d.line([27, 36, 47, 28], fill=nota, width=4)
    d.line([47, 28, 47, 40], fill=nota, width=4)
    return img


def build_tray(port: int, loop: asyncio.AbstractEventLoop, client_ref: list):
    if not _TRAY_AVAILABLE:
        return None

    def on_open(_):
        webbrowser.open(f"http://localhost:{port}")

    def on_quit(_):
        icon.stop()
        c = client_ref[0] if client_ref else None
        if c:
            asyncio.run_coroutine_threadsafe(c.disconnect(), loop)
        time.sleep(0.5)
        os._exit(0)

    icon = pystray.Icon(
        "metrowrap",
        _make_icon(False),
        "metrowrap",
        menu=pystray.Menu(
            pystray.MenuItem("Arayuzu Ac", on_open, default=True),
            pystray.MenuItem("Cikis",      on_quit),
        ),
    )

    def _updater():
        last = None
        while True:
            time.sleep(1.5)
            connected = state.status == "connected"
            if connected != last:
                icon.icon  = _make_icon(connected)
                icon.title = (
                    f"metrowrap · {state.room_code}" if connected else "metrowrap"
                )
                last = connected

    threading.Thread(target=_updater, daemon=True).start()
    return icon


# ── Web UI HTML ───────────────────────────────────────────────────────────────

UI_HTML = r"""<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>metrowrap</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:     #0D1117; --s1: #161B22; --s2: #1E2430; --s3: #252B38; --s4: #2E3444;
  --border: rgba(255,255,255,0.07);
  --fg:     #E6EDF3; --fg2: #8B92A8; --fg3: #4A5068;
  --accent: #7FBBB3; --green: #A7C080; --red: #E67E80; --yellow: #DBBC7F;
  --r: 12px; --r-lg: 20px; --f: 'Outfit', system-ui, sans-serif;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--fg);font-family:var(--f);font-size:14px;line-height:1.5;overflow:hidden}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--s4);border-radius:4px}
.app{display:grid;grid-template-rows:52px 1fr;grid-template-columns:272px 1fr;height:100vh}
.topbar{grid-column:1/-1;background:var(--s1);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 20px;gap:14px}
.logo{font-size:15px;font-weight:600;letter-spacing:-.03em}.logo em{color:var(--accent);font-style:normal}
.topbar-right{margin-left:auto;display:flex;align-items:center;gap:8px;font-size:12px;color:var(--fg2)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--fg3);flex-shrink:0;transition:background .4s,box-shadow .4s}
.dot.connected{background:var(--green);box-shadow:0 0 8px var(--green)}
.dot.connecting,.dot.waiting{background:var(--yellow);animation:blink 1.2s ease-in-out infinite}
.dot.error{background:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.sidebar{background:var(--s1);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.sec-label{padding:16px 18px 7px;font-size:10px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:var(--fg3)}
.cform{padding:0 14px 16px;display:flex;flex-direction:column;gap:9px}
.field label{display:block;font-size:11px;font-weight:500;color:var(--fg2);margin-bottom:4px}
input{width:100%;background:var(--s2);border:1px solid var(--border);border-radius:var(--r);color:var(--fg);font-family:var(--f);font-size:13px;padding:8px 12px;outline:none;transition:border-color .2s,background .2s}
input:focus{border-color:var(--accent);background:var(--s3)}input::placeholder{color:var(--fg3)}
.btn{width:100%;padding:9px 16px;border-radius:var(--r);border:none;font-family:var(--f);font-size:13px;font-weight:500;cursor:pointer;transition:all .15s;letter-spacing:.02em}
.btn:active{transform:scale(.97)}
.btn-conn{background:var(--accent);color:#0D1117}.btn-conn:hover{filter:brightness(1.08)}.btn-conn:disabled{background:var(--s4);color:var(--fg3);cursor:not-allowed}
.btn-disc{background:transparent;border:1px solid var(--red);color:var(--red)}.btn-disc:hover{background:var(--red);color:var(--bg)}
.users-wrap{flex:1;overflow-y:auto;padding:0 10px 12px}
.uitem{display:flex;align-items:center;gap:10px;padding:7px 8px;border-radius:var(--r);transition:background .15s}.uitem:hover{background:var(--s2)}
.uavatar{width:30px;height:30px;border-radius:50%;background:var(--s3);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;color:var(--accent);flex-shrink:0;border:1.5px solid transparent;transition:border-color .3s}
.uavatar.on{border-color:var(--green)}.uavatar.off{opacity:.45}
.uname{font-size:13px;flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ubadge{font-size:9px;font-weight:600;background:rgba(219,188,127,.13);color:var(--yellow);border-radius:5px;padding:2px 6px;letter-spacing:.06em}
.empty{color:var(--fg3);font-size:12px;padding:8px 8px}
.main{display:flex;flex-direction:column;overflow:hidden}
.nowplay{position:relative;flex-shrink:0;overflow:hidden}
.np-bg{position:absolute;inset:0;background-size:cover;background-position:center;filter:blur(48px) brightness(.25) saturate(1.8);transform:scale(1.15);transition:background-image .8s ease}
.np-bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(13,17,23,.1) 0%,rgba(13,17,23,.85) 100%)}
.np-inner{position:relative;z-index:1;display:flex;align-items:flex-end;gap:20px;padding:28px 24px 0}
.art{width:90px;height:90px;border-radius:16px;background:var(--s3);flex-shrink:0;overflow:hidden;display:flex;align-items:center;justify-content:center;font-size:30px;color:var(--fg3);box-shadow:0 8px 28px rgba(0,0,0,.5);transition:box-shadow .6s}
.art.playing{animation:artglow 4s ease-in-out infinite}
@keyframes artglow{0%,100%{box-shadow:0 8px 28px rgba(0,0,0,.5)}50%{box-shadow:0 8px 36px rgba(127,187,179,.22)}}
.art img{width:100%;height:100%;object-fit:cover}
.tmeta{flex:1;min-width:0;padding-bottom:4px}
.ttitle{font-size:16px;font-weight:600;letter-spacing:-.025em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.3}
.tartist{font-size:13px;color:var(--fg2);margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.talbum{font-size:11px;color:var(--fg3);margin-top:1px}
.tright{display:flex;flex-direction:column;align-items:flex-end;gap:4px;padding-bottom:4px;flex-shrink:0}
.picon{font-size:22px;color:var(--green);transition:color .3s}.picon.paused{color:var(--fg3)}
.pvol{font-size:11px;color:var(--fg2)}
.prog-wrap{position:relative;z-index:1;padding:14px 24px 18px}
.prog-track{height:4px;background:rgba(255,255,255,.1);border-radius:4px;cursor:pointer;position:relative;transition:height .2s}.prog-track:hover{height:6px}
.prog-fill{height:100%;border-radius:4px;background:var(--accent);width:0%;position:relative}
.prog-fill::after{content:'';position:absolute;right:-5px;top:50%;transform:translateY(-50%);width:12px;height:12px;border-radius:50%;background:var(--accent);opacity:0;transition:opacity .2s}
.prog-track:hover .prog-fill::after{opacity:1}
.prog-times{display:flex;justify-content:space-between;margin-top:7px;font-size:11px;color:var(--fg2);font-variant-numeric:tabular-nums;letter-spacing:.02em}

/* Buffering göstergesi */
.buf-bar{height:2px;background:transparent;position:relative;z-index:1;margin:-2px 24px 0}
.buf-fill{height:100%;background:rgba(127,187,179,.35);width:0%;border-radius:2px;transition:width .3s}

.tabs{display:flex;border-bottom:1px solid var(--border);padding:0 24px;background:var(--bg);flex-shrink:0}
.tab{padding:10px 14px;font-size:12px;font-family:var(--f);font-weight:500;cursor:pointer;color:var(--fg3);background:transparent;border:none;border-bottom:2px solid transparent;transition:color .15s,border-color .15s;letter-spacing:.03em}.tab:hover{color:var(--fg)}.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.pane{flex:1;overflow-y:auto;padding:10px 24px;display:none}.pane.show{display:block}
.qitem{display:flex;align-items:center;gap:12px;padding:7px 8px;border-radius:var(--r);transition:background .15s}.qitem:hover{background:var(--s1)}
.qnum{font-size:11px;color:var(--fg3);width:20px;text-align:right;flex-shrink:0;font-variant-numeric:tabular-nums}
.qthumb{width:38px;height:38px;border-radius:8px;background:var(--s2);flex-shrink:0;overflow:hidden;display:flex;align-items:center;justify-content:center;font-size:14px;color:var(--fg3)}
.qthumb img{width:100%;height:100%;object-fit:cover}
.qinfo{flex:1;min-width:0}.qtitle{font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.qartist{font-size:11px;color:var(--fg2)}
.loglist{font-size:11px}
.lentry{display:grid;grid-template-columns:56px 1fr;gap:8px;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.03)}
.lentry.error .lmsg{color:var(--red)}.lentry.warn .lmsg{color:var(--yellow)}
.lt{color:var(--fg3)}.lmsg{color:var(--fg2);word-break:break-word}

/* Cache badge */
.cache-badge{font-size:9px;background:rgba(167,192,128,.15);color:var(--green);border-radius:4px;padding:1px 5px;margin-left:6px;vertical-align:middle;letter-spacing:.05em}
</style>
</head>
<body>
<div class="app">

<header class="topbar">
  <span class="logo">metro<em>wrap</em></span>
  <div class="topbar-right">
    <div class="dot" id="dot"></div>
    <span id="smsg">Bağlı değil</span>
    <button id="btnQuit" title="Kapat" style="margin-left:8px;background:transparent;border:1px solid var(--fg3);border-radius:6px;color:var(--fg3);font-family:var(--f);font-size:11px;padding:3px 9px;cursor:pointer;transition:all .15s;" onmouseover="this.style.borderColor='var(--red)';this.style.color='var(--red)';" onmouseout="this.style.borderColor='var(--fg3)';this.style.color='var(--fg3)';">kapat</button>
  </div>
</header>

<aside class="sidebar">
  <div class="sec-label">Bağlantı</div>
  <div class="cform">
    <div class="field"><label>Oda Kodu</label><input id="iRoom" placeholder="ABCD1234" maxlength="10" style="text-transform:uppercase;letter-spacing:.1em"></div>
    <div class="field"><label>Kullanıcı Adı</label><input id="iUser" placeholder="PC" value="PC"></div>
    <div class="field"><label>Sunucu</label><input id="iSrv" placeholder="wss://..."></div>
    <button class="btn btn-conn" id="btnC">Bağlan</button>
    <button class="btn btn-disc" id="btnD" style="display:none">Bağlantıyı Kes</button>
  </div>
  <div class="sec-label">Dinleyiciler</div>
  <div class="users-wrap" id="users"><div class="empty">Henüz kimse yok</div></div>
</aside>

<main class="main">
  <!-- Gizli <audio> elementi — tüm ses buradan -->
  <audio id="player" preload="auto" crossorigin="anonymous"></audio>

  <div class="nowplay">
    <div class="np-bg" id="npbg"></div>
    <div class="np-inner">
      <div class="art" id="art">♪</div>
      <div class="tmeta">
        <div class="ttitle" id="tTitle">—</div>
        <div class="tartist" id="tArtist">Bağlantı bekleniyor</div>
        <div class="talbum" id="tAlbum"></div>
      </div>
      <div class="tright">
        <div class="picon paused" id="picon">⏸</div>
        <div class="pvol" id="pvol">🔊 100%</div>
      </div>
    </div>
    <div class="prog-wrap">
      <div class="prog-track" id="progTrack">
        <div class="prog-fill" id="progFill"></div>
      </div>
      <div class="prog-times"><span id="pCur">0:00</span><span id="pDur">0:00</span></div>
    </div>
    <div class="buf-bar"><div class="buf-fill" id="bufFill"></div></div>
  </div>

  <div class="tabs">
    <button class="tab active" data-tab="queue">Sıra</button>
    <button class="tab" data-tab="log">Log</button>
  </div>
  <div class="pane show" id="pane-queue"><div id="queueList"><div class="empty">Sıra boş</div></div></div>
  <div class="pane" id="pane-log"><div class="loglist" id="logList"></div></div>
</main>

</div>

<script>
// ── Player ──────────────────────────────────────────────────────────────────
const player = document.getElementById('player');
let currentSrc = '';
let pendingPlay = false;
let pendingPos  = 0;
let currentDur  = 0;
let lastThumb   = '';
let logScroll   = true;
let lastVer     = -1;

// Progress güncellemesi — audio zamanından al
function tickProgress() {
  if (!player.src || player.duration < 1) return;
  const pos = player.currentTime * 1000;
  const dur = player.duration * 1000;
  currentDur = dur;
  document.getElementById('progFill').style.width = Math.min(100, pos / dur * 100) + '%';
  document.getElementById('pCur').textContent = ms(pos);
  document.getElementById('pDur').textContent = ms(dur);

  // Buffered göstergesi
  if (player.buffered.length > 0) {
    const buffEnd = player.buffered.end(player.buffered.length - 1);
    document.getElementById('bufFill').style.width = Math.min(100, buffEnd / player.duration * 100) + '%';
  }
}
setInterval(tickProgress, 500);

function execCmd(cmd) {
  switch (cmd.op) {
    case 'load': {
      const newSrc = cmd.src + '?t=' + Date.now(); // cache-bust for fresh fetch
      if (currentSrc !== cmd.src) {
        currentSrc  = cmd.src;
        player.src  = newSrc;
        player.load();
      }
      pendingPos  = cmd.pos_ms || 0;
      pendingPlay = cmd.play;
      // canplay sonrası seek + play
      player.oncanplay = () => {
        player.oncanplay = null;
        if (pendingPos > 0) player.currentTime = pendingPos / 1000;
        if (pendingPlay) player.play().catch(() => {});
        else player.pause();
      };
      break;
    }
    case 'play': {
      if (cmd.pos_ms != null) {
        const target = cmd.pos_ms / 1000;
        if (Math.abs(player.currentTime - target) > 1.5)
          player.currentTime = target;
      }
      player.play().catch(() => {});
      break;
    }
    case 'pause': {
      if (cmd.pos_ms != null) {
        const target = cmd.pos_ms / 1000;
        if (Math.abs(player.currentTime - target) > 1.5)
          player.currentTime = target;
      }
      player.pause();
      break;
    }
    case 'seek':
      if (cmd.pos_ms != null) player.currentTime = cmd.pos_ms / 1000;
      break;
    case 'volume':
      player.volume = Math.max(0, Math.min(1, cmd.v || 1));
      break;
  }
}

// ── State render ─────────────────────────────────────────────────────────────
function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function ms(v) {
  if (!v || v <= 0) return '0:00';
  const s = Math.floor(v / 1000), m = Math.floor(s / 60);
  return m + ':' + String(s % 60).padStart(2, '0');
}

function render(s) {
  // Anlık komutları çalıştır
  if (s.cmds && s.cmds.length) s.cmds.forEach(execCmd);

  document.getElementById('dot').className  = 'dot ' + s.status;
  document.getElementById('smsg').textContent = s.status_msg || '';

  const busy = ['connected','connecting','waiting'].includes(s.status);
  document.getElementById('btnC').style.display = busy ? 'none' : '';
  document.getElementById('btnD').style.display = busy ? '' : 'none';
  document.getElementById('btnC').disabled = false;

  const t   = s.current_track;
  const art = document.getElementById('art');

  if (!t) {
    document.getElementById('tTitle').textContent  = '—';
    document.getElementById('tArtist').textContent = s.status === 'connected' ? 'Şarkı bekleniyor...' : 'Bağlantı bekleniyor';
    document.getElementById('tAlbum').textContent  = '';
    art.innerHTML = '♪'; art.className = 'art';
    document.getElementById('npbg').style.backgroundImage = '';
    document.getElementById('picon').textContent = '⏸';
    document.getElementById('picon').className   = 'picon paused';
    document.getElementById('progFill').style.width = '0%';
    document.getElementById('pCur').textContent = '0:00';
    document.getElementById('pDur').textContent = '0:00';
  } else {
    document.getElementById('tTitle').textContent  = t.title  || '—';
    document.getElementById('tArtist').textContent = t.artist || '';
    document.getElementById('tAlbum').textContent  = t.album  || '';

    if (t.thumbnail !== lastThumb) {
      lastThumb = t.thumbnail || '';
      if (t.thumbnail) {
        art.innerHTML = '<img src="' + esc(t.thumbnail) + '" alt="">';
        document.getElementById('npbg').style.backgroundImage = 'url(' + esc(t.thumbnail) + ')';
      } else {
        art.innerHTML = '♪';
        document.getElementById('npbg').style.backgroundImage = '';
      }
    }
    art.className = 'art' + (s.is_playing ? ' playing' : '');

    const pi = document.getElementById('picon');
    pi.textContent = s.is_playing ? '▶' : '⏸';
    pi.className   = 'picon' + (s.is_playing ? '' : ' paused');
  }

  document.getElementById('pvol').textContent = '🔊 ' + Math.round((s.volume || 1) * 100) + '%';

  // Users
  const ul = document.getElementById('users');
  ul.innerHTML = (s.users && s.users.length)
    ? s.users.map(u => {
        const init = (u.name || '?').slice(0,2).toUpperCase();
        return '<div class="uitem">' +
          '<div class="uavatar ' + (u.connected ? 'on' : 'off') + '">' + esc(init) + '</div>' +
          '<span class="uname">' + esc(u.name) + '</span>' +
          (u.is_host ? '<span class="ubadge">HOST</span>' : '') +
          '</div>';
      }).join('')
    : '<div class="empty">Henüz kimse yok</div>';

  // Queue
  const ql = document.getElementById('queueList');
  ql.innerHTML = (s.queue && s.queue.length)
    ? s.queue.map((q, i) =>
        '<div class="qitem">' +
        '<span class="qnum">' + (i+1) + '</span>' +
        '<div class="qthumb">' + (q.thumbnail ? '<img src="'+esc(q.thumbnail)+'" alt="">' : '♪') + '</div>' +
        '<div class="qinfo"><div class="qtitle">' + esc(q.title) + '</div>' +
        '<div class="qartist">' + esc(q.artist||'') + '</div></div></div>'
      ).join('')
    : '<div class="empty">Sıra boş</div>';

  // Log
  const ll = document.getElementById('logList');
  ll.innerHTML = (s.logs||[]).map(l =>
    '<div class="lentry ' + (l.level||'') + '">' +
    '<span class="lt">' + esc(l.t) + '</span>' +
    '<span class="lmsg">' + esc(l.msg) + '</span></div>'
  ).join('');
  if (logScroll) {
    const el = document.getElementById('pane-log');
    el.scrollTop = el.scrollHeight;
  }
}

// ── SSE ──────────────────────────────────────────────────────────────────────
function startSSE() {
  const es = new EventSource('/api/events');
  es.onmessage = ev => {
    const s = JSON.parse(ev.data);
    if (s.version !== lastVer) { lastVer = s.version; render(s); }
  };
  es.onerror = () => {
    // Bağlantı koptu, polling'e geç
    es.close();
    setInterval(async () => {
      const s = await fetchState();
      if (s && s.version !== lastVer) { lastVer = s.version; render(s); }
    }, 1500);
  };
}

async function fetchState() {
  try { const r = await fetch('/api/state'); return r.ok ? r.json() : null; } catch { return null; }
}

async function doConnect() {
  const room = document.getElementById('iRoom').value.trim().toUpperCase();
  const user = document.getElementById('iUser').value.trim() || 'PC';
  const srv  = document.getElementById('iSrv').value.trim();
  if (!room) { alert('Oda kodu gerekli'); return; }
  if (!srv)  { alert('Sunucu URL gerekli'); return; }
  document.getElementById('btnC').disabled = true;
  await fetch('/api/connect', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ room_code: room, username: user, server_url: srv }),
  });
}
async function doDisconnect() {
  await fetch('/api/disconnect', { method: 'POST' });
}
async function doQuit() {
  player.pause();
  await fetch('/api/quit', { method: 'POST' }).catch(() => {});
  document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;font-family:var(--f);color:var(--fg2);font-size:14px;">metrowrap kapatıldı</div>';
}

async function init() {
  const s = await fetchState();
  if (s) document.getElementById('iSrv').value = s.server_url || '';

  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.pane').forEach(p => p.classList.remove('show'));
      btn.classList.add('active');
      document.getElementById('pane-' + btn.dataset.tab).classList.add('show');
    });
  });

  document.getElementById('pane-log').addEventListener('scroll', () => {
    const el = document.getElementById('pane-log');
    logScroll = el.scrollTop + el.clientHeight >= el.scrollHeight - 20;
  });

  document.getElementById('btnC').addEventListener('click', doConnect);
  document.getElementById('btnD').addEventListener('click', doDisconnect);
  document.getElementById('iRoom').addEventListener('input', ev => ev.target.value = ev.target.value.toUpperCase());
  document.getElementById('btnQuit').addEventListener('click', doQuit);

  // Player olayları → log satırı
  player.addEventListener('waiting',  () => state_log('Tampon dolduruluyor…'));
  player.addEventListener('playing',  () => state_log(''));
  player.addEventListener('error',    () => state_log('Ses hatası: ' + (player.error && player.error.message)));

  function state_log(msg) {
    // UI-only log, sunucuya gönderilmez
    if (msg) console.log('[player]', msg);
  }

  startSSE();
}

init();
</script>
</body>
</html>"""

# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(title="metrowrap", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_client_ref: list = []


@app.get("/", response_class=HTMLResponse)
async def ui_root():
    return HTMLResponse(UI_HTML)


@app.get("/api/state")
async def api_state():
    return JSONResponse(state.snapshot())


@app.get("/api/events")
async def api_events():
    async def gen():
        last = -1
        while True:
            snap = state.snapshot()
            v = snap["version"]
            if v != last or snap["is_playing"] or snap["cmds"]:
                last = v
                # Komutları bir kez gönder, sonra temizle
                state.pop_cmds()
                yield f"data: {json.dumps(snap)}\n\n"
            await asyncio.sleep(0.4)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/stream/{track_id}")
async def api_stream(track_id: str, request: Request):
    """
    Ses akışı:
    1. Cache'te hazır dosya varsa → Range destekli disk stream
    2. Yoksa → yt-dlp ile anlık URL al, proxy et + arka planda cache'e indir
    """
    # Güvenlik: sadece alfanumerik track id
    if not track_id.replace("-", "").replace("_", "").isalnum():
        return Response(status_code=400)

    cached_path = cache.get_path(track_id)
    if cached_path:
        state.add_log(f"Cache hit: {track_id[:8]}…")
        return await _stream_file(cached_path, request)

    # Cache miss — URL al ve proxy et, aynı zamanda arka planda indir
    state.add_log(f"Cache miss, yt-dlp cagiriliyor: {track_id[:8]}…")

    # URL alma işlemi birkaç saniye sürebilir, thread'de yap
    loop = asyncio.get_event_loop()
    url = await loop.run_in_executor(None, cache.get_stream_url, track_id)

    if not url:
        return Response(status_code=502, content="Stream URL alinamadi")

    # Arka planda cache'e indir (eğer henüz başlamadıysa)
    cache.start_download(track_id)

    return await _proxy_yt_stream(url, request)


@app.post("/api/connect")
async def api_connect(body: dict):
    room = body.get("room_code", "").upper().strip()
    user = (body.get("username") or "PC").strip()
    srv  = (body.get("server_url") or "").strip()
    if not room:
        return JSONResponse({"ok": False, "error": "room_code gerekli"}, 400)
    if not srv:
        return JSONResponse({"ok": False, "error": "server_url gerekli"}, 400)
    if _client_ref:
        await _client_ref[0].connect(srv, room, user)
    return JSONResponse({"ok": True})


@app.post("/api/disconnect")
async def api_disconnect():
    if _client_ref:
        await _client_ref[0].disconnect()
    return JSONResponse({"ok": True})


@app.post("/api/quit")
async def api_quit():
    if _client_ref:
        try:
            await _client_ref[0].disconnect()
        except:
            pass
    asyncio.get_event_loop().call_later(0.3, os._exit, 0)
    return JSONResponse({"ok": True})


# ── Ana döngü ─────────────────────────────────────────────────────────────────

async def _async_main(port: int):
    client = MetroClient()
    _client_ref.append(client)

    uv_config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    uv_server = uvicorn.Server(uv_config)
    asyncio.create_task(uv_server.serve())

    state.add_log(f"Web arayuzu: http://localhost:{port}")
    state.add_log(f"Cache dizini: {cache.dir}")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


def main():
    parser = ArgumentParser(description="Metrolist Listen Together PC Wrapper")
    parser.add_argument("--port",      type=int, default=DEFAULT_PORT)
    parser.add_argument("--server",    default=DEFAULT_SERVER)
    parser.add_argument("--no-tray",   action="store_true")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    args = parser.parse_args()

    # Cache başlat
    global cache
    cache = TrackCache(Path(args.cache_dir))

    state.update(server_url=args.server)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(_async_main(args.port))

    use_tray = not args.no_tray and _TRAY_AVAILABLE
    if use_tray:
        icon = build_tray(args.port, loop, _client_ref)
        tray_thread = threading.Thread(
            target=lambda: webbrowser.open(f"http://localhost:{args.port}") or icon.run(),
            daemon=True,
        )
        tray_thread.start()
    else:
        if not args.no_tray and not _TRAY_AVAILABLE:
            print("[UYARI] Tray kullanilamiyor, --no-tray modunda devam ediliyor.")
        print(f"Web UI: http://localhost:{args.port}")
        print(f"Cache:  {args.cache_dir}")
        print("Cikis icin Ctrl+C")

    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        print("\nCikiliyor...")
        task.cancel()
        loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
