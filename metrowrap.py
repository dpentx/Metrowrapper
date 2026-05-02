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

# pystray: Windows/macOS'ta her zaman calısır.
# Linux'ta masaustu ortami (GTK) gerekir; yoksa --no-tray moduna otomatik dusulur.
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
        self.status     = "idle"        # idle | connecting | waiting | connected | error
        self.status_msg = "Bagli degil"
        self.room_code  = ""
        self.username   = ""
        self.server_url = DEFAULT_SERVER
        self.user_id    = ""
        self.is_playing = False
        self.position_ms    = 0
        self.position_ts    = 0.0       # time.monotonic() referansi
        self.volume         = 1.0
        self.current_track  = None      # dict veya None
        self.users          = []        # list[dict]
        self.queue          = []        # list[dict]
        self.logs           = []        # list[dict]
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

    def start(self):
        args = [
            "mpv", "--idle=yes", "--no-terminal", "--no-video",
            f"--input-ipc-server={self._ipc}",
            "--ytdl=yes", "--ytdl-format=bestaudio",
        ]
        try:
            self._proc = subprocess.Popen(
                args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            state.add_log("mpv baslatildi")
            time.sleep(1.0)
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
        for _ in range(15):
            try:
                if sys.platform == "win32":
                    self._sock = None  # Windows: open() ile yaziyoruz
                    return
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(self._ipc)
                self._sock = s
                return
            except (ConnectionRefusedError, FileNotFoundError, OSError):
                time.sleep(0.3)
        state.add_log("mpv IPC baglanamadi", "warn")

    def _cmd(self, cmd: list):
        msg = (json.dumps({"command": cmd}) + "\n").encode()
        with self._lock:
            try:
                if sys.platform == "win32":
                    with open(self._ipc, "wb", buffering=0) as p:
                        p.write(msg)
                elif self._sock:
                    self._sock.sendall(msg)
            except Exception as e:
                state.add_log(f"mpv IPC hata: {e}", "warn")
                self._connect()

    def load(self, track_id: str):
        url = f"https://music.youtube.com/watch?v={track_id}"
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

    # ── Mesaj isleme ──────────────────────────────────────────────────────────

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
        queue = [{"id": q.id, "title": q.title, "artist": q.artist} for q in obj.queue]
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
        self.mpv.load(proto_track.id)
        await asyncio.sleep(0.5)
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
    color = (167, 192, 128) if connected else (133, 146, 137)  # Everforest yesil / gri
    d.ellipse([4, 4, size - 4, size - 4], fill=color)
    # Nota sekli
    nota = (45, 53, 59)
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
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&display=swap');
:root{
  --bg0:#2D353B;--bg1:#343F44;--bg2:#3D484D;--bg3:#475258;
  --fg:#D3C6AA;--fg2:#859289;
  --green:#A7C080;--blue:#7FBBB3;--yellow:#DBBC7F;
  --orange:#E69875;--red:#E67E80;--aqua:#83C092;
  --r:6px;--f:'JetBrains Mono',monospace;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg0);color:var(--fg);font-family:var(--f);font-size:13px;line-height:1.6}

.shell{display:grid;grid-template-rows:48px 1fr;grid-template-columns:270px 1fr;height:100vh}

/* topbar */
.topbar{grid-column:1/-1;background:var(--bg1);border-bottom:1px solid var(--bg3);
  display:flex;align-items:center;padding:0 18px;gap:10px}
.logo{font-size:15px;font-weight:600;color:var(--green);letter-spacing:.05em}
.logo em{color:var(--fg2);font-style:normal;font-weight:300}
.top-right{margin-left:auto;display:flex;align-items:center;gap:8px;font-size:12px;color:var(--fg2)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--bg3);transition:background .4s}
.dot.connected{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot.waiting,.dot.connecting{background:var(--yellow);animation:blink 1s infinite}
.dot.error{background:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}

/* sidebar */
.sidebar{background:var(--bg1);border-right:1px solid var(--bg3);
  display:flex;flex-direction:column;overflow:hidden}
.slabel{padding:14px 16px 6px;font-size:10px;font-weight:600;
  letter-spacing:.12em;color:var(--fg2);text-transform:uppercase}
.cform{padding:0 14px 14px;display:flex;flex-direction:column;gap:8px}
.flabel{font-size:11px;color:var(--fg2);margin-bottom:2px;display:block}
input{width:100%;background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--r);
  color:var(--fg);font-family:var(--f);font-size:12px;padding:6px 10px;outline:none;
  transition:border-color .2s}
input:focus{border-color:var(--blue)}
input::placeholder{color:var(--fg2)}
.btn{width:100%;padding:7px 14px;border-radius:var(--r);border:none;
  font-family:var(--f);font-size:12px;font-weight:500;cursor:pointer;
  transition:opacity .15s,transform .1s;letter-spacing:.04em}
.btn:active{transform:scale(.97)}
.btn-c{background:var(--green);color:var(--bg0)}
.btn-c:hover{opacity:.85}
.btn-c:disabled{background:var(--bg3);color:var(--fg2);cursor:default}
.btn-d{background:transparent;border:1px solid var(--red);color:var(--red);margin-top:2px}
.btn-d:hover{background:var(--red);color:var(--bg0)}

.users{flex:1;overflow-y:auto;padding:0 14px}
.uitem{display:flex;align-items:center;gap:8px;padding:5px 0;font-size:12px;
  border-bottom:1px solid var(--bg2)}
.uitem:last-child{border:none}
.udot{width:6px;height:6px;border-radius:50%;background:var(--bg3);flex-shrink:0}
.udot.on{background:var(--green)}.udot.off{background:var(--bg3)}
.ubadge{margin-left:auto;font-size:9px;color:var(--yellow);
  border:1px solid var(--yellow);border-radius:3px;padding:1px 4px}
.empty{color:var(--fg2);font-size:12px;padding:8px 0;font-style:italic}

/* main */
.main{display:flex;flex-direction:column;overflow:hidden}
.nowplay{background:var(--bg1);border-bottom:1px solid var(--bg3);
  padding:18px 22px;display:flex;align-items:center;gap:16px;min-height:112px}
.thumb{width:70px;height:70px;border-radius:var(--r);background:var(--bg2);
  flex-shrink:0;border:1px solid var(--bg3);display:flex;align-items:center;
  justify-content:center;font-size:28px;color:var(--bg3);overflow:hidden}
.thumb img{width:100%;height:100%;object-fit:cover}
.tmeta{flex:1;min-width:0}
.ttitle{font-size:15px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tartist{font-size:12px;color:var(--fg2);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.talbum{font-size:11px;color:var(--bg3);margin-top:1px}
.pinfo{display:flex;flex-direction:column;align-items:flex-end;gap:5px;flex-shrink:0}
.pstate{font-size:22px;color:var(--green)}
.pstate.paused{color:var(--fg2)}
.ptime,.pvol{font-size:11px;color:var(--fg2);letter-spacing:.02em}

/* tabs */
.tabs{display:flex;border-bottom:1px solid var(--bg3);padding:0 22px;background:var(--bg0)}
.tab{padding:10px 16px;font-size:12px;cursor:pointer;color:var(--fg2);
  border-bottom:2px solid transparent;transition:color .15s,border-color .15s;user-select:none}
.tab:hover{color:var(--fg)}.tab.active{color:var(--green);border-bottom-color:var(--green)}
.pane{flex:1;overflow-y:auto;padding:14px 22px;display:none}
.pane.active{display:block}

/* queue */
.qitem{display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--bg2)}
.qitem:last-child{border:none}
.qnum{color:var(--fg2);font-size:11px;width:20px;text-align:right;flex-shrink:0}
.qinfo{flex:1;min-width:0}
.qtitle{font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.qartist{font-size:11px;color:var(--fg2)}

/* log */
.loglist{font-size:11px}
.lentry{display:grid;grid-template-columns:56px 1fr;gap:8px;padding:3px 0;
  border-bottom:1px solid rgba(255,255,255,.03)}
.lt{color:var(--fg2)}.lm{color:var(--fg);word-break:break-word}
.lentry.error .lm{color:var(--red)}.lentry.warn .lm{color:var(--yellow)}

::-webkit-scrollbar{width:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--bg3);border-radius:3px}
</style>
</head>
<body>
<div class="shell">

<header class="topbar">
  <span class="logo">metro<em>wrap</em></span>
  <div class="top-right">
    <div class="dot" id="dot"></div>
    <span id="smsg">Bagli degil</span>
  </div>
</header>

<aside class="sidebar">
  <div class="slabel">Baglanti</div>
  <div class="cform">
    <div><label class="flabel">Oda Kodu</label>
      <input id="iRoom" placeholder="ABCD1234" maxlength="10"
             style="text-transform:uppercase;letter-spacing:.1em"></div>
    <div><label class="flabel">Kullanici Adi</label>
      <input id="iUser" placeholder="PC" value="PC"></div>
    <div><label class="flabel">Sunucu</label>
      <input id="iSrv" placeholder="wss://..."></div>
    <button class="btn btn-c" id="btnC">Baglan</button>
    <button class="btn btn-d" id="btnD" style="display:none">Baglantıyı Kes</button>
  </div>

  <div class="slabel">Dinleyiciler</div>
  <div class="users" id="users"><div class="empty">Henuz kimse yok</div></div>
</aside>

<main class="main">
  <div class="nowplay">
    <div class="thumb" id="thumb">♪</div>
    <div class="tmeta">
      <div class="ttitle" id="tTitle">—</div>
      <div class="tartist" id="tArtist">Baglanti bekleniyor</div>
      <div class="talbum" id="tAlbum"></div>
    </div>
    <div class="pinfo">
      <div class="pstate paused" id="pState">⏸</div>
      <div class="ptime" id="pTime">—:—— / —:——</div>
      <div class="pvol" id="pVol">🔊 100%</div>
    </div>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="queue">Sira</div>
    <div class="tab" data-tab="log">Log</div>
  </div>
  <div class="pane active" id="tab-queue">
    <div id="queueList"><div class="empty">Sira bos</div></div>
  </div>
  <div class="pane" id="tab-log">
    <div class="loglist" id="logList"></div>
  </div>
</main>

</div>
<script>
let lastVer=-1,logScroll=true;

async function init(){
  const s=await get();
  if(s){document.getElementById('iSrv').value=s.server_url||'';}
  document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>{
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    document.querySelectorAll('.pane').forEach(x=>x.classList.remove('active'));
    t.classList.add('active');
    document.getElementById('tab-'+t.dataset.tab).classList.add('active');
  }));
  document.getElementById('tab-log').addEventListener('scroll',()=>{
    const el=document.getElementById('tab-log');
    logScroll=el.scrollTop+el.clientHeight>=el.scrollHeight-20;
  });
  document.getElementById('btnC').addEventListener('click',doConnect);
  document.getElementById('btnD').addEventListener('click',doDisconnect);
  document.getElementById('iRoom').addEventListener('input',e=>e.target.value=e.target.value.toUpperCase());
  startSSE();
}

function startSSE(){
  const es=new EventSource('/api/events');
  es.onmessage=e=>{const s=JSON.parse(e.data);if(s.version!==lastVer){lastVer=s.version;render(s);}};
  es.onerror=()=>setInterval(async()=>{const s=await get();if(s&&s.version!==lastVer){lastVer=s.version;render(s);}},1500);
}

async function get(){try{const r=await fetch('/api/state');return r.ok?r.json():null;}catch{return null;}}

async function doConnect(){
  const room=document.getElementById('iRoom').value.trim().toUpperCase();
  const user=document.getElementById('iUser').value.trim()||'PC';
  const srv=document.getElementById('iSrv').value.trim();
  if(!room){alert('Oda kodu gerekli');return;}
  if(!srv){alert('Sunucu URL gerekli');return;}
  document.getElementById('btnC').disabled=true;
  await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({room_code:room,username:user,server_url:srv})});
}

async function doDisconnect(){
  await fetch('/api/disconnect',{method:'POST'});
}

function render(s){
  // status
  const dot=document.getElementById('dot');
  dot.className='dot '+s.status;
  document.getElementById('smsg').textContent=s.status_msg||'';
  const connected=s.status==='connected';
  const busy=connected||s.status==='connecting'||s.status==='waiting';
  document.getElementById('btnC').style.display=busy?'none':'';
  document.getElementById('btnD').style.display=busy?'':'none';
  document.getElementById('btnC').disabled=false;

  // track
  const t=s.current_track;
  if(!t){
    document.getElementById('tTitle').textContent='—';
    document.getElementById('tArtist').textContent=connected?'Sarki bekleniyor...':'Baglanti bekleniyor';
    document.getElementById('tAlbum').textContent='';
    document.getElementById('thumb').innerHTML='♪';
    document.getElementById('thumb').style.background='';
    document.getElementById('pState').textContent='⏸';
    document.getElementById('pState').className='pstate paused';
    document.getElementById('pTime').textContent='—:—— / —:——';
  }else{
    document.getElementById('tTitle').textContent=t.title||'—';
    document.getElementById('tArtist').textContent=t.artist||'';
    document.getElementById('tAlbum').textContent=t.album||'';
    const th=document.getElementById('thumb');
    if(t.thumbnail){
      th.innerHTML=`<img src="${e(t.thumbnail)}" alt="">`;
      th.style.background='';
    }else{
      th.innerHTML='♪';th.style.background='';
    }
    document.getElementById('pState').textContent=s.is_playing?'▶':'⏸';
    document.getElementById('pState').className='pstate'+(s.is_playing?'':' paused');
    document.getElementById('pTime').textContent=`${ms(s.position_ms)} / ${ms(t.duration_ms)}`;
  }
  document.getElementById('pVol').textContent='🔊 '+Math.round((s.volume||1)*100)+'%';

  // users
  const ul=document.getElementById('users');
  ul.innerHTML=(s.users&&s.users.length)?s.users.map(u=>
    `<div class="uitem"><div class="udot ${u.connected?'on':'off'}"></div>`+
    `<span>${e(u.name)}</span>${u.is_host?'<span class="ubadge">HOST</span>':''}</div>`
  ).join(''):'<div class="empty">Henuz kimse yok</div>';

  // queue
  const ql=document.getElementById('queueList');
  ql.innerHTML=(s.queue&&s.queue.length)?s.queue.map((q,i)=>
    `<div class="qitem"><span class="qnum">${i+1}</span>`+
    `<div class="qinfo"><div class="qtitle">${e(q.title)}</div>`+
    `<div class="qartist">${e(q.artist||'')}</div></div></div>`
  ).join(''):'<div class="empty">Sira bos</div>';

  // logs
  const ll=document.getElementById('logList');
  ll.innerHTML=(s.logs||[]).map(l=>
    `<div class="lentry ${l.level||''}"><span class="lt">${e(l.t)}</span><span class="lm">${e(l.msg)}</span></div>`
  ).join('');
  if(logScroll){const p=document.getElementById('tab-log');p.scrollTop=p.scrollHeight;}
}

function ms(v){
  if(!v||v<0)return'—:——';
  const s=Math.floor(v/1000),m=Math.floor(s/60);
  return m+':'+String(s%60).padStart(2,'0');
}
function e(s){
  if(!s)return'';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

init();
</script>
</body>
</html>"""

# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(title="metrowrap", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_client_ref: list = []   # [MetroClient]  — tray'e de erişim için


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
            v = state.version
            if v != last:
                last = v
                yield f"data: {json.dumps(state.snapshot())}\n\n"
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
            print("[UYARI] Tray kullaниlamıyor (GTK eksik?), --no-tray modunda devam ediliyor.")
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
