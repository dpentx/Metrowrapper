"""
metrowrap.py — Metrolist Listen Together PC Wrapper
Tray + Web UI (localhost:7823)

Bagimliliklar:
    pip install websockets protobuf fastapi uvicorn pystray Pillow yt-dlp

Kullanim:
    python metrowrap.py
    python metrowrap.py --port 7823 --server wss://metroserver.nyxie.dev/ws
    python metrowrap.py --no-tray   # tray olmadan (terminal modunda)
"""

import asyncio
import gzip
import json
import os
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Optional

# ── Protobuf ──────────────────────────────────────────────────────────────────
try:
    import listentogether_pb2 as pb
except ImportError:
    print("[HATA] listentogether_pb2.py bulunamadi. Proto'yu compile edin.")
    sys.exit(1)

# ── Dis kutuphaneler ──────────────────────────────────────────────────────────
try:
    import websockets
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    from PIL import Image, ImageDraw
except ImportError as e:
    print(f"[HATA] Eksik kutuphane: {e}")
    print("Cozum: pip install websockets protobuf fastapi uvicorn pystray Pillow")
    sys.exit(1)

_TRAY_AVAILABLE = False
try:
    import pystray
    _TRAY_AVAILABLE = True
except Exception:
    pass

# ── Sabitler ──────────────────────────────────────────────────────────────────

DEFAULT_SERVER = "wss://metroserverx.meowery.eu/ws"
DEFAULT_PORT   = 7823

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

# ── Uygulama durumu (thread-safe) ─────────────────────────────────────────────

class AppState:
    def __init__(self):
        self._lock = threading.Lock()
        self.status     = "idle"
        self.status_msg = "Bagli degil"
        self.room_code  = ""
        self.username   = ""
        self.server_url = DEFAULT_SERVER
        self.user_id    = ""
        self.is_playing = False
        self.position_ms    = 0
        self.position_ts    = 0.0
        self.volume         = 1.0
        self.current_track  = None
        self.users          = []
        self.queue          = []
        self.logs           = []
        self._version       = 0

    def update(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)
            self._version += 1

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


# ── mpv koprusu ───────────────────────────────────────────────────────────────

class MpvBridge:
    def __init__(self):
        self._proc = None
        self._sock = None
        self._lock = threading.Lock()
        if sys.platform == "win32":
            self._ipc = r"\\.\pipe\metrowrap"
        else:
            self._ipc = "/tmp/metrowrap.sock"

    @staticmethod
    def _find_ytdlp() -> str:
        import shutil
        found = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
        if not found:
            scripts = os.path.join(os.path.dirname(sys.executable), "Scripts")
            candidate = os.path.join(scripts, "yt-dlp.exe")
            if os.path.exists(candidate):
                found = candidate
        if not found:
            state.add_log("yt-dlp bulunamadi! pip install yt-dlp", "error")
            return ""
        return found

    def start(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_mpv  = os.path.join(script_dir, "mpv.exe")
        mpv_bin    = local_mpv if os.path.exists(local_mpv) else "mpv"
        args = [
            mpv_bin, "--idle=yes", "--no-terminal", "--no-video",
            f"--input-ipc-server={self._ipc}",
            "--ao=pulse",
            "--ytdl-format=bestaudio/best",
            f"--log-file={os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mpv.log')}",
        ]
        try:
            self._proc = subprocess.Popen(
                args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            state.add_log("mpv baslatildi")
            time.sleep(1.2)
            self._connect()
        except FileNotFoundError:
            state.add_log("mpv bulunamadi! Kurun: https://mpv.io", "error")

    def stop(self):
        self._cmd(["quit"])
        if self._sock:
            try: self._sock.close()
            except: pass
        if self._proc:
            try: self._proc.terminate()
            except: pass

    def _connect(self):
        """Socket'in hazır olduğunu doğrula (her cmd kendi bağlantısını açar)."""
        if sys.platform == "win32":
            import ctypes
            GENERIC_WRITE  = 0x40000000
            OPEN_EXISTING  = 3
            INVALID_HANDLE = ctypes.c_void_p(-1).value
            k32 = ctypes.windll.kernel32
            for _ in range(20):
                h = k32.CreateFileW(self._ipc, GENERIC_WRITE, 0, None, OPEN_EXISTING, 0x80, None)
                if h != INVALID_HANDLE:
                    k32.CloseHandle(h)
                    state.add_log("mpv IPC hazir")
                    return
                time.sleep(0.3)
            state.add_log("mpv named pipe acilamadi", "warn")
            return
        for i in range(20):
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(1.0)
                s.connect(self._ipc)
                s.close()
                state.add_log("mpv IPC hazir")
                return
            except (ConnectionRefusedError, FileNotFoundError, OSError):
                time.sleep(0.3)
        state.add_log("mpv IPC baglanamadi — ses calmayabilir", "warn")

    def _cmd(self, cmd: list):
        msg = (json.dumps({"command": cmd}) + "\n").encode()
        with self._lock:
            try:
                if sys.platform == "win32":
                    self._win32_write(msg)
                else:
                    # Her komutta yeni baglanti ac; kalici socket
                    # kopmalarinda komutlar sessizce dusmez.
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.settimeout(2.0)
                    s.connect(self._ipc)
                    s.sendall(msg)
                    s.close()
            except Exception as e:
                state.add_log(f"mpv IPC hata: {e}", "warn")

    def _win32_write(self, data: bytes):
        import ctypes
        GENERIC_WRITE  = 0x40000000
        OPEN_EXISTING  = 3
        INVALID_HANDLE = ctypes.c_void_p(-1).value
        k32 = ctypes.windll.kernel32
        h = k32.CreateFileW(self._ipc, GENERIC_WRITE, 0, None, OPEN_EXISTING, 0x80, None)
        if h == INVALID_HANDLE:
            raise OSError(f"CreateFile failed err={k32.GetLastError()}")
        try:
            written = ctypes.c_ulong(0)
            k32.WriteFile(h, data, len(data), ctypes.byref(written), None)
        finally:
            k32.CloseHandle(h)

    def load(self, track_id: str, stream_url: str = ""):
        # ytdl:// protokolü ile YouTube Music akışını yükle
        # mpv'nin built-in yt-dlp desteğini kullan
        url = f"ytdl://https://music.youtube.com/watch?v={track_id}"
        self._cmd(["loadfile", url, "replace"])

    def play(self, pos_ms: Optional[int] = None):
        if pos_ms is not None:
            self.seek(pos_ms)
        self._cmd(["set_property", "pause", False])

    def pause(self, pos_ms: Optional[int] = None):
        if pos_ms is not None:
            self.seek(pos_ms)
        self._cmd(["set_property", "pause", True])

    def seek(self, pos_ms: int):
        self._cmd(["seek", pos_ms / 1000.0, "absolute"])

    def set_volume(self, v: float):
        self._cmd(["set_property", "volume", v * 100])


# ── WebSocket istemcisi ───────────────────────────────────────────────────────

class MetroClient:
    def __init__(self, mpv: MpvBridge):
        self.mpv = mpv
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
        self.mpv.pause()
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
            except: pass
        if self._task:
            self._task.cancel()
            try: await self._task
            except: pass
        self._ws = None

    async def _run(self, server_url: str, room_code: str, username: str):
        try:
            async with websockets.connect(
                server_url, ping_interval=None, close_timeout=5
            ) as ws:
                self._ws = ws
                state.add_log(f"Sunucuya baglandi")

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
            except: break

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
            self.mpv.pause()
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
                self.mpv.play(pos)
            else:
                self.mpv.pause(pos)
            state.update(
                is_playing=obj.is_playing, position_ms=pos,
                position_ts=time.monotonic() if obj.is_playing else 0,
            )

        self.mpv.set_volume(obj.volume)
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
            self.mpv.play(pos)
            state.update(is_playing=True, position_ms=pos, position_ts=time.monotonic())
            state.add_log("Oynatiliyor")
        elif action == ACTION_PAUSE:
            self.mpv.pause(obj.position)
            state.update(is_playing=False, position_ms=obj.position, position_ts=0)
            state.add_log("Duraklatildi")
        elif action == ACTION_SEEK:
            self.mpv.seek(obj.position)
            state.update(position_ms=obj.position, position_ts=time.monotonic())
        elif action == ACTION_SET_VOLUME:
            self.mpv.set_volume(obj.volume)
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

        # ytdl:// protokolü ile yükle - yt-dlp'yi mpv otomatik çalıştıracak
        self.mpv.load(proto_track.id, "")

        await asyncio.sleep(1.5)  # Stream buffering için bekle
        if pos_ms > 0:
            self.mpv.seek(pos_ms)
        if play:
            self.mpv.play()
        else:
            self.mpv.pause()
        try:
            await self._ws.send(encode_msg(
                MSG_BUFFER_READY,
                pb.BufferReadyPayload(track_id=proto_track.id),
            ))
        except: pass


def _live_pos(position_ms: int, last_update_ms: int, is_playing: bool) -> int:
    if not is_playing or not last_update_ms:
        return position_ms
    now_ms = int(time.time() * 1000)
    return max(0, position_ms + (now_ms - last_update_ms))


# ── Tray ikonu ────────────────────────────────────────────────────────────────

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
            pystray.MenuItem("Cikis", on_quit),
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


# ── Web UI ────────────────────────────────────────────────────────────────────

UI_HTML = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>metrowrap</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:       #0D1117;
  --s1:       #161B22;
  --s2:       #1E2430;
  --s3:       #252B38;
  --s4:       #2E3444;
  --border:   rgba(255,255,255,0.07);
  --fg:       #E6EDF3;
  --fg2:      #8B92A8;
  --fg3:      #4A5068;
  --accent:   #7FBBB3;
  --green:    #A7C080;
  --red:      #E67E80;
  --yellow:   #DBBC7F;
  --r:        12px;
  --r-lg:     20px;
  --r-xl:     28px;
  --f:        'Outfit', system-ui, sans-serif;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; background: var(--bg); color: var(--fg); font-family: var(--f); font-size: 14px; line-height: 1.5; overflow: hidden; }
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--s4); border-radius: 4px; }

.app {
  display: grid;
  grid-template-rows: 52px 1fr;
  grid-template-columns: 272px 1fr;
  height: 100vh;
}

/* ── topbar ── */
.topbar {
  grid-column: 1 / -1;
  background: var(--s1);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  padding: 0 20px;
  gap: 14px;
}
.logo {
  font-size: 15px;
  font-weight: 600;
  letter-spacing: -0.03em;
  color: var(--fg);
}
.logo em { color: var(--accent); font-style: normal; }
.topbar-right {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: var(--fg2);
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--fg3);
  flex-shrink: 0;
  transition: background .4s, box-shadow .4s;
}
.dot.connected  { background: var(--green); box-shadow: 0 0 8px var(--green); }
.dot.connecting,
.dot.waiting    { background: var(--yellow); animation: blink 1.2s ease-in-out infinite; }
.dot.error      { background: var(--red); }
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:.25} }

/* ── sidebar ── */
.sidebar {
  background: var(--s1);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.sec-label {
  padding: 16px 18px 7px;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--fg3);
}
.cform {
  padding: 0 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 9px;
}
.field label {
  display: block;
  font-size: 11px;
  font-weight: 500;
  color: var(--fg2);
  margin-bottom: 4px;
}
input {
  width: 100%;
  background: var(--s2);
  border: 1px solid var(--border);
  border-radius: var(--r);
  color: var(--fg);
  font-family: var(--f);
  font-size: 13px;
  padding: 8px 12px;
  outline: none;
  transition: border-color .2s, background .2s;
}
input:focus { border-color: var(--accent); background: var(--s3); }
input::placeholder { color: var(--fg3); }

.btn {
  width: 100%;
  padding: 9px 16px;
  border-radius: var(--r);
  border: none;
  font-family: var(--f);
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all .15s;
  letter-spacing: .02em;
}
.btn:active { transform: scale(.97); }
.btn-conn  { background: var(--accent); color: #0D1117; }
.btn-conn:hover { filter: brightness(1.08); }
.btn-conn:disabled { background: var(--s4); color: var(--fg3); cursor: not-allowed; }
.btn-disc  { background: transparent; border: 1px solid var(--red); color: var(--red); }
.btn-disc:hover { background: var(--red); color: var(--bg); }

.users-wrap { flex: 1; overflow-y: auto; padding: 0 10px 12px; }
.uitem {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 7px 8px;
  border-radius: var(--r);
  transition: background .15s;
}
.uitem:hover { background: var(--s2); }
.uavatar {
  width: 30px;
  height: 30px;
  border-radius: 50%;
  background: var(--s3);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  font-weight: 600;
  color: var(--accent);
  flex-shrink: 0;
  border: 1.5px solid transparent;
  transition: border-color .3s;
}
.uavatar.on  { border-color: var(--green); }
.uavatar.off { opacity: .45; }
.uname { font-size: 13px; flex: 1; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.ubadge {
  font-size: 9px;
  font-weight: 600;
  background: rgba(219,188,127,.13);
  color: var(--yellow);
  border-radius: 5px;
  padding: 2px 6px;
  letter-spacing: .06em;
}
.empty { color: var(--fg3); font-size: 12px; padding: 8px 8px; }

/* ── main ── */
.main { display: flex; flex-direction: column; overflow: hidden; }

/* now playing */
.nowplay {
  position: relative;
  flex-shrink: 0;
  overflow: hidden;
}
.np-bg {
  position: absolute;
  inset: 0;
  background-size: cover;
  background-position: center;
  filter: blur(48px) brightness(.25) saturate(1.8);
  transform: scale(1.15);
  transition: background-image .8s ease;
}
.np-bg::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(180deg, rgba(13,17,23,.1) 0%, rgba(13,17,23,.85) 100%);
}
.np-inner {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: flex-end;
  gap: 20px;
  padding: 28px 24px 0;
}
.art {
  width: 90px;
  height: 90px;
  border-radius: 16px;
  background: var(--s3);
  flex-shrink: 0;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 30px;
  color: var(--fg3);
  box-shadow: 0 8px 28px rgba(0,0,0,.5);
  transition: box-shadow .6s;
}
.art.playing {
  animation: artglow 4s ease-in-out infinite;
}
@keyframes artglow {
  0%,100% { box-shadow: 0 8px 28px rgba(0,0,0,.5); }
  50%      { box-shadow: 0 8px 36px rgba(127,187,179,.22); }
}
.art img { width: 100%; height: 100%; object-fit: cover; }

.tmeta { flex: 1; min-width: 0; padding-bottom: 4px; }
.ttitle {
  font-size: 16px;
  font-weight: 600;
  letter-spacing: -.025em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  line-height: 1.3;
}
.tartist { font-size: 13px; color: var(--fg2); margin-top: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.talbum  { font-size: 11px; color: var(--fg3); margin-top: 1px; }

.tright {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
  padding-bottom: 4px;
  flex-shrink: 0;
}
.picon { font-size: 22px; color: var(--green); transition: color .3s; }
.picon.paused { color: var(--fg3); }
.pvol { font-size: 11px; color: var(--fg2); }

/* progress */
.prog-wrap {
  position: relative;
  z-index: 1;
  padding: 14px 24px 18px;
}
.prog-track {
  height: 4px;
  background: rgba(255,255,255,.1);
  border-radius: 4px;
  cursor: pointer;
  position: relative;
  transition: height .2s;
}
.prog-track:hover { height: 6px; }
.prog-fill {
  height: 100%;
  border-radius: 4px;
  background: var(--accent);
  width: 0%;
  transition: width .5s linear;
  position: relative;
}
.prog-fill::after {
  content: '';
  position: absolute;
  right: -5px;
  top: 50%;
  transform: translateY(-50%);
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: var(--accent);
  opacity: 0;
  transition: opacity .2s;
}
.prog-track:hover .prog-fill::after { opacity: 1; }
.prog-times {
  display: flex;
  justify-content: space-between;
  margin-top: 7px;
  font-size: 11px;
  color: var(--fg2);
  font-variant-numeric: tabular-nums;
  letter-spacing: .02em;
}

/* tabs */
.tabs {
  display: flex;
  border-bottom: 1px solid var(--border);
  padding: 0 24px;
  background: var(--bg);
  flex-shrink: 0;
}
.tab {
  padding: 10px 14px;
  font-size: 12px;
  font-family: var(--f);
  font-weight: 500;
  cursor: pointer;
  color: var(--fg3);
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  transition: color .15s, border-color .15s;
  letter-spacing: .03em;
}
.tab:hover { color: var(--fg); }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }

.pane {
  flex: 1;
  overflow-y: auto;
  padding: 10px 24px;
  display: none;
}
.pane.show { display: block; }

/* queue */
.qitem {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 7px 8px;
  border-radius: var(--r);
  transition: background .15s;
}
.qitem:hover { background: var(--s1); }
.qnum { font-size: 11px; color: var(--fg3); width: 20px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }
.qthumb {
  width: 38px;
  height: 38px;
  border-radius: 8px;
  background: var(--s2);
  flex-shrink: 0;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  color: var(--fg3);
}
.qthumb img { width: 100%; height: 100%; object-fit: cover; }
.qinfo { flex: 1; min-width: 0; }
.qtitle  { font-size: 13px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.qartist { font-size: 11px; color: var(--fg2); }

/* log */
.loglist { font-size: 11px; }
.lentry {
  display: grid;
  grid-template-columns: 56px 1fr;
  gap: 8px;
  padding: 3px 0;
  border-bottom: 1px solid rgba(255,255,255,.03);
}
.lentry.error .lmsg { color: var(--red); }
.lentry.warn  .lmsg { color: var(--yellow); }
.lt   { color: var(--fg3); }
.lmsg { color: var(--fg2); word-break: break-word; }
</style>
</head>
<body>
<div class="app">

<header class="topbar">
  <span class="logo">metro<em>wrap</em></span>
  <div class="topbar-right">
    <div class="dot" id="dot"></div>
    <span id="smsg">Bağlı değil</span>
    <button id="btnQuit" title="Kapat (Ctrl+A)" style="margin-left:8px;background:transparent;border:1px solid var(--fg3);border-radius:6px;color:var(--fg3);font-family:var(--f);font-size:11px;padding:3px 9px;cursor:pointer;transition:all .15s;" onmouseover="this.style.borderColor='var(--red)';this.style.color='var(--red)';" onmouseout="this.style.borderColor='var(--fg3)';this.style.color='var(--fg3)';">kapat</button>
  </div>
</header>

<aside class="sidebar">
  <div class="sec-label">Bağlantı</div>
  <div class="cform">
    <div class="field">
      <label>Oda Kodu</label>
      <input id="iRoom" placeholder="ABCD1234" maxlength="10" style="text-transform:uppercase;letter-spacing:.1em">
    </div>
    <div class="field">
      <label>Kullanıcı Adı</label>
      <input id="iUser" placeholder="PC" value="PC">
    </div>
    <div class="field">
      <label>Sunucu</label>
      <input id="iSrv" placeholder="wss://...">
    </div>
    <button class="btn btn-conn" id="btnC">Bağlan</button>
    <button class="btn btn-disc" id="btnD" style="display:none">Bağlantıyı Kes</button>
  </div>

  <div class="sec-label">Dinleyiciler</div>
  <div class="users-wrap" id="users">
    <div class="empty">Henüz kimse yok</div>
  </div>
</aside>

<main class="main">
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
      <div class="prog-times">
        <span id="pCur">0:00</span>
        <span id="pDur">0:00</span>
      </div>
    </div>
  </div>

  <div class="tabs">
    <button class="tab active" data-tab="queue">Sıra</button>
    <button class="tab" data-tab="log">Log</button>
  </div>
  <div class="pane show" id="pane-queue">
    <div id="queueList"><div class="empty">Sıra boş</div></div>
  </div>
  <div class="pane" id="pane-log">
    <div class="loglist" id="logList"></div>
  </div>
</main>

</div>
<script>
let lastVer = -1, logScroll = true, lastThumb = '';

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function ms(v) {
  if (!v || v <= 0) return '0:00';
  const s = Math.floor(v / 1000), m = Math.floor(s / 60);
  return m + ':' + String(s % 60).padStart(2, '0');
}

async function init() {
  const s = await fetchState();
  if (s) document.getElementById('iSrv').value = s.server_url || '';

  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const tab = btn.dataset.tab;
      document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.pane').forEach(p => p.classList.remove('show'));
      btn.classList.add('active');
      document.getElementById('pane-' + tab).classList.add('show');
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
  document.addEventListener('keydown', ev => {
    if (ev.ctrlKey && ev.key === 'a') { ev.preventDefault(); doQuit(); }
  });
  startSSE();
}

function startSSE() {
  const es = new EventSource('/api/events');
  es.onmessage = ev => {
    const s = JSON.parse(ev.data);
    if (s.version !== lastVer) { lastVer = s.version; render(s); }
  };
  es.onerror = () => {
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
    body: JSON.stringify({ room_code: room, username: user, server_url: srv })
  });
}
async function doDisconnect() {
  await fetch('/api/disconnect', { method: 'POST' });
}
async function doQuit() {
  await fetch('/api/quit', { method: 'POST' }).catch(() => {});
  document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;font-family:var(--f);color:var(--fg2);font-size:14px;">metrowrap kapatıldı</div>';
}

function render(s) {
  document.getElementById('dot').className = 'dot ' + s.status;
  document.getElementById('smsg').textContent = s.status_msg || '';

  const busy = ['connected','connecting','waiting'].includes(s.status);
  document.getElementById('btnC').style.display = busy ? 'none' : '';
  document.getElementById('btnD').style.display = busy ? '' : 'none';
  document.getElementById('btnC').disabled = false;

  const t = s.current_track;
  const art = document.getElementById('art');

  if (!t) {
    document.getElementById('tTitle').textContent  = '—';
    document.getElementById('tArtist').textContent = s.status === 'connected' ? 'Şarkı bekleniyor...' : 'Bağlantı bekleniyor';
    document.getElementById('tAlbum').textContent  = '';
    art.innerHTML = '♪';
    art.className = 'art';
    document.getElementById('npbg').style.backgroundImage = '';
    document.getElementById('picon').textContent = '⏸';
    document.getElementById('picon').className = 'picon paused';
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
    pi.className = 'picon' + (s.is_playing ? '' : ' paused');

    const pos = s.position_ms || 0;
    const dur = t.duration_ms || 1;
    document.getElementById('progFill').style.width = Math.min(100, pos / dur * 100) + '%';
    document.getElementById('pCur').textContent = ms(pos);
    document.getElementById('pDur').textContent = ms(dur);
  }

  document.getElementById('pvol').textContent = '🔊 ' + Math.round((s.volume || 1) * 100) + '%';

  // users
  const ul = document.getElementById('users');
  ul.innerHTML = (s.users && s.users.length)
    ? s.users.map(u => {
        const init = (u.name || '?').slice(0, 2).toUpperCase();
        return '<div class="uitem">' +
          '<div class="uavatar ' + (u.connected ? 'on' : 'off') + '">' + esc(init) + '</div>' +
          '<span class="uname">' + esc(u.name) + '</span>' +
          (u.is_host ? '<span class="ubadge">HOST</span>' : '') +
          '</div>';
      }).join('')
    : '<div class="empty">Henüz kimse yok</div>';

  // queue
  const ql = document.getElementById('queueList');
  ql.innerHTML = (s.queue && s.queue.length)
    ? s.queue.map((q, i) =>
        '<div class="qitem">' +
        '<span class="qnum">' + (i + 1) + '</span>' +
        '<div class="qthumb">' + (q.thumbnail ? '<img src="' + esc(q.thumbnail) + '" alt="">' : '♪') + '</div>' +
        '<div class="qinfo"><div class="qtitle">' + esc(q.title) + '</div>' +
        '<div class="qartist">' + esc(q.artist || '') + '</div></div>' +
        '</div>'
      ).join('')
    : '<div class="empty">Sıra boş</div>';

  // log
  const ll = document.getElementById('logList');
  ll.innerHTML = (s.logs || []).map(l =>
    '<div class="lentry ' + (l.level || '') + '">' +
    '<span class="lt">' + esc(l.t) + '</span>' +
    '<span class="lmsg">' + esc(l.msg) + '</span>' +
    '</div>'
  ).join('');
  if (logScroll) {
    const el = document.getElementById('pane-log');
    el.scrollTop = el.scrollHeight;
  }
}

init();
</script>
</body>
</html>"""

# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(title="metrowrap", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
            # version degismese bile oynarken pozisyon guncellenir
            if v != last or snap["is_playing"]:
                last = v
                yield f"data: {json.dumps(snap)}\n\n"
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")


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
        try: await _client_ref[0].disconnect()
        except: pass
    # Kisa gecikme sonrasi sureci sonlandir
    asyncio.get_event_loop().call_later(0.3, os._exit, 0)
    return JSONResponse({"ok": True})


# ── Ana döngü ─────────────────────────────────────────────────────────────────

async def _async_main(port: int):
    mpv    = MpvBridge()
    client = MetroClient(mpv)
    _client_ref.append(client)
    mpv.start()

    uv_config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    uv_server  = uvicorn.Server(uv_config)
    asyncio.create_task(uv_server.serve())

    state.add_log(f"Web arayuzu: http://localhost:{port}")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        mpv.stop()


def main():
    parser = ArgumentParser(description="Metrolist Listen Together PC Wrapper")
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--server",  default=DEFAULT_SERVER)
    parser.add_argument("--no-tray", action="store_true")
    args = parser.parse_args()

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
            print("[UYARI] Tray kullanilamiyor (GTK eksik?), --no-tray modunda devam ediliyor.")
        print(f"Web UI: http://localhost:{args.port}")
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
