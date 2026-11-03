#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
====================================================================
       Discover news
====================================================================
Discord Screen Share / Stream Spoofer (Cloud / .env Edition)
All Rights Reserved to Discord User: Discover news
Official Discord Server: https://discord.gg/cjXbGqVCzq
====================================================================
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import ssl
import struct
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Optional

# محاولة تحميل مكتبة dotenv إن وجدت محلياً
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

AUTHOR_DISCORD = "Discover"
SERVER_INVITE = "https://discord.gg/cjXbGqVCzq"

DISCORD_API = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "discord/1.0.9168 Chrome/128.0.6613.186 Electron/32.2.2 Safari/537.36"
)

STREAM_BADGE_TIERS = [
    (1, "Tier 1: Newcomer (1 hour)"),
    (5, "Tier 2: Fledgling (5 hours)"),
    (20, "Tier 3: Breakout (20 hours)"),
    (75, "Tier 4: Standout (75 hours)"),
    (150, "Tier 5: Trendsetter (150 hours)"),
    (300, "Tier 6: Headliner (300 hours)"),
    (500, "Tier 7: Star (500 hours)"),
    (1000, "Tier 8: Sensation (1,000 hours)"),
    (2000, "Tier 9: Visionary (2,000 hours)"),
    (5000, "Tier 10: Phenomenon (5,000+ hours)"),
]


def make_super_properties() -> str:
    props = {
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": "stable",
        "client_version": "1.0.9168",
        "os_version": "10.0.22631",
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": "en-US",
        "browser_user_agent": USER_AGENT,
        "browser_version": "32.2.2",
        "client_build_number": 343349,
        "native_build_number": 53915,
        "client_event_source": None,
    }
    return base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()


class AsyncWebSocket:
    def __init__(self, url: str) -> None:
        self.url = url
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.closed = False
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        parsed = re.match(r"^(wss?|https?)://([^/:]+)(?::(\d+))?(/.*)?$", self.url)
        if not parsed:
            raise ValueError(f"Invalid WebSocket URL: {self.url}")

        scheme, host, port_str, path = parsed.groups()
        use_ssl = scheme in ("wss", "https")
        port = int(port_str) if port_str else (443 if use_ssl else 80)
        path = path or "/"

        ssl_ctx = ssl.create_default_context() if use_ssl else None
        self.reader, self.writer = await asyncio.open_connection(
            host, port, ssl=ssl_ctx, server_hostname=host if use_ssl else None
        )

        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"User-Agent: {USER_AGENT}\r\n"
            f"Origin: https://discord.com\r\n"
            f"\r\n"
        )
        self.writer.write(handshake.encode("utf-8"))
        await self.writer.drain()

        status_line = await self.reader.readline()
        if not status_line.startswith(b"HTTP/1.1 101"):
            raise ConnectionError(f"WebSocket upgrade failed: {status_line.decode(errors='ignore').strip()}")

        while True:
            header_line = await self.reader.readline()
            if header_line in (b"\r\n", b"\n", b""):
                break

    async def send_json(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await self._send_frame(0x1, data)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed or not self.writer:
            return
        async with self._write_lock:
            header = bytearray()
            header.append(0x80 | (opcode & 0x0F))
            length = len(payload)
            mask_bit = 0x80
            if length < 126:
                header.append(mask_bit | length)
            elif length < 65536:
                header.append(mask_bit | 126)
                header.extend(struct.pack("!H", length))
            else:
                header.append(mask_bit | 127)
                header.extend(struct.pack("!Q", length))

            mask_key = os.urandom(4)
            header.extend(mask_key)
            masked_payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
            self.writer.write(header + masked_payload)
            await self.writer.drain()

    async def recv_json(self) -> Optional[dict[str, Any]]:
        if not self.reader or self.closed:
            return None

        fragments = bytearray()
        message_opcode: Optional[int] = None

        while True:
            first_two = await self.reader.readexactly(2)
            b1, b2 = first_two[0], first_two[1]
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F

            if length == 126:
                ext = await self.reader.readexactly(2)
                length = struct.unpack("!H", ext)[0]
            elif length == 127:
                ext = await self.reader.readexactly(8)
                length = struct.unpack("!Q", ext)[0]

            mask_key = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length) if length > 0 else b""
            if masked:
                payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))

            if opcode == 0x8:
                self.closed = True
                raise ConnectionResetError("Server closed connection")
            elif opcode == 0x9:
                await self._send_frame(0xA, payload)
                continue
            elif opcode == 0xA:
                continue
            elif opcode in (0x1, 0x2):
                message_opcode = opcode
                fragments.extend(payload)
            elif opcode == 0x0:
                fragments.extend(payload)

            if fin:
                if message_opcode == 0x1:
                    return json.loads(fragments.decode("utf-8", errors="replace"))
                fragments.clear()
                message_opcode = None

    async def close(self) -> None:
        if self.closed:
            return
        if self.writer:
            try:
                await self._send_frame(0x8, struct.pack("!H", 1000))
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.closed = True


def discord_api_get(endpoint: str, token: str, api_base: str = DISCORD_API) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{api_base}{endpoint}",
        headers={
            "Authorization": token.strip(),
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties(),
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_room_input(raw_input: str, token: str, api_base: str = DISCORD_API) -> tuple[Optional[str], str, str]:
    text = raw_input.strip()
    match_url = re.search(r"discord(?:app)?\.com/channels/(@me|\d+)/(\d+)", text)
    if match_url:
        g_part, c_id = match_url.group(1), match_url.group(2)
        guild_id = None if g_part == "@me" else g_part
        try:
            ch_info = discord_api_get(f"/channels/{c_id}", token, api_base=api_base)
            ch_name = ch_info.get("name") or f"DM/Call ({c_id})"
            guild_id = ch_info.get("guild_id", guild_id)
            return guild_id, c_id, ch_name
        except Exception:
            return guild_id, c_id, f"Channel {c_id}"

    match_pair = re.match(r"^(\d{15,22})[:/\s]+(\d{15,22})$", text)
    if match_pair:
        g_id, c_id = match_pair.group(1), match_pair.group(2)
        return g_id, c_id, f"Channel {c_id}"

    match_id = re.search(r"(\d{15,22})", text)
    if not match_id:
        raise ValueError("Invalid room ID or URL.")
    channel_id = match_id.group(1)
    ch_info = discord_api_get(f"/channels/{channel_id}", token, api_base=api_base)
    return ch_info.get("guild_id"), channel_id, ch_info.get("name") or f"Channel {channel_id}"


class RTCVoiceSession:
    def __init__(self, label: str, endpoint: str, server_id: str, channel_id: str, user_id: str, session_id: str, voice_token: str, is_stream: bool = False) -> None:
        self.label = label
        self.endpoint = endpoint
        self.server_id = server_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.session_id = session_id
        self.voice_token = voice_token
        self.is_stream = is_stream
        self.ws: Optional[AsyncWebSocket] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._hb_task: Optional[asyncio.Task[None]] = None
        self.running = False

    def start(self) -> None:
        self.running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self.running = False
        if self._hb_task:
            self._hb_task.cancel()
        if self.ws:
            await self.ws.close()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        url = self.endpoint
        if not url.startswith("wss://") and not url.startswith("ws://"):
            url = f"wss://{url}"
        if "?v=" not in url:
            url = f"{url.rstrip('/')}/?v=8"

        try:
            self.ws = AsyncWebSocket(url)
            await self.ws.connect()
            await self.ws.send_json({
                "op": 0,
                "d": {
                    "server_id": str(self.server_id),
                    "channel_id": str(self.channel_id),
                    "user_id": str(self.user_id),
                    "session_id": self.session_id,
                    "token": self.voice_token,
                    "video": True,
                    "streams": [{"type": "screen" if self.is_stream else "video", "rid": "100", "quality": 100}]
                }
            })
            while self.running:
                msg = await self.ws.recv_json()
                if not msg:
                    break
                op, d = msg.get("op"), msg.get("d") or {}
                if op == 8:
                    interval = d.get("heartbeat_interval", 15000) / 1000.0
                    if self._hb_task:
                        self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._heartbeat_loop(interval))
                elif op == 2:
                    await self.ws.send_json({
                        "op": 1,
                        "d": {
                            "protocol": "udp",
                            "data": {"address": d.get("ip", "127.0.0.1"), "port": d.get("port", 1337), "mode": "aead_xchacha20_poly1305_rtpsize"},
                            "codecs": [{"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120}],
                            "rtc_connection_id": str(uuid.uuid4())
                        }
                    })
        except Exception:
            pass

    async def _heartbeat_loop(self, interval: float) -> None:
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": -1}})
                await asyncio.sleep(interval)
        except Exception:
            pass


class DiscordStreamClient:
    def __init__(self, token: str, guild_id: Optional[str], channel_id: str, role: str = "streamer") -> None:
        self.token = token.strip()
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.role = role
        self.ws: Optional[AsyncWebSocket] = None
        self.seq: Optional[int] = None
        self.user_id: Optional[str] = None
        self.username: str = "Unknown"
        self.voice_session_id: Optional[str] = None
        self.stream_key: Optional[str] = None
        self.stream_active: bool = False
        self.running: bool = False
        self.voice_rtc: Optional[RTCVoiceSession] = None
        self.stream_rtc: Optional[RTCVoiceSession] = None
        self.ready_event = asyncio.Event()
        self.stream_created_event = asyncio.Event()

    def build_stream_key(self) -> str:
        if self.guild_id:
            return f"guild:{self.guild_id}:{self.channel_id}:{self.user_id}"
        return f"call:{self.channel_id}:{self.user_id}"

    async def run_forever(self) -> None:
        self.running = True
        while self.running:
            try:
                await self._connect_and_listen()
            except Exception:
                await asyncio.sleep(5)

    async def _connect_and_listen(self) -> None:
        self.ws = AsyncWebSocket(GATEWAY_URL)
        await self.ws.connect()
        hb_task = None
        try:
            while self.running:
                msg = await self.ws.recv_json()
                if not msg:
                    break
                op, t, s, d = msg.get("op"), msg.get("t"), msg.get("s"), msg.get("d")
                if s is not None:
                    self.seq = s
                if op == 10:
                    hb_task = asyncio.create_task(self._gateway_heartbeat_loop((d or {}).get("heartbeat_interval", 41250) / 1000.0))
                    await self.ws.send_json({
                        "op": 2,
                        "d": {
                            "token": self.token,
                            "capabilities": 16381,
                            "properties": {"os": "Windows", "browser": "Discord Client", "release_channel": "stable"},
                            "presence": {"status": "online", "since": 0, "activities": [], "afk": False}
                        }
                    })
                elif op == 0:
                    await self._handle_dispatch(t, d or {})
        finally:
            if hb_task:
                hb_task.cancel()
            if self.ws:
                await self.ws.close()

    async def _handle_dispatch(self, event_name: Optional[str], d: dict[str, Any]) -> None:
        if event_name == "READY":
            user = d.get("user") or {}
            self.user_id, self.username = user.get("id"), user.get("username", "User")
            self.voice_session_id = d.get("session_id")
            print(f"[+] Logged in ({self.role}): {self.username}")
            await self.ws.send_json({
                "op": 4,
                "d": {"guild_id": self.guild_id, "channel_id": self.channel_id, "self_mute": True, "self_deaf": True, "self_video": False}
            })
            self.ready_event.set()
            if self.role == "streamer":
                await asyncio.sleep(1.5)
                await self.ws.send_json({
                    "op": 18,
                    "d": {"type": "guild" if self.guild_id else "call", "guild_id": self.guild_id, "channel_id": self.channel_id, "preferred_region": None}
                })
                self.stream_key = self.build_stream_key()
                await self.ws.send_json({"op": 22, "d": {"stream_key": self.stream_key, "paused": False}})

        elif event_name == "VOICE_SERVER_UPDATE":
            if d.get("endpoint") and d.get("token") and self.user_id:
                self.voice_rtc = RTCVoiceSession(f"Voice-{self.username}", d.get("endpoint"), str(d.get("guild_id") or self.channel_id), str(self.channel_id), str(self.user_id), self.voice_session_id, d.get("token"))
                self.voice_rtc.start()

        elif event_name == "STREAM_CREATE":
            if d.get("stream_key"):
                self.stream_key = d.get("stream_key")
                self.stream_active = True
                self.stream_created_event.set()

        elif event_name == "STREAM_SERVER_UPDATE":
            if d.get("endpoint") and d.get("token") and self.user_id and self.stream_key:
                self.stream_rtc = RTCVoiceSession(f"Stream-{self.username}", d.get("endpoint"), str(self.guild_id or self.channel_id), str(self.channel_id), str(self.user_id), self.voice_session_id, d.get("token"), is_stream=True)
                self.stream_rtc.start()

    async def _gateway_heartbeat_loop(self, interval: float) -> None:
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 1, "d": self.seq})
                await asyncio.sleep(interval)
        except Exception:
            pass


async def main() -> None:
    # جلب الإعدادات من متغيرات البيئة أو ملف .env
    token = os.getenv("DISCORD_TOKEN", "").strip()
    viewer_token = os.getenv("VIEWER_TOKEN", "").strip()
    room_input = os.getenv("ROOM_INPUT", "").strip()

    if not token or not room_input:
        print("[!] Error: Please configure DISCORD_TOKEN and ROOM_INPUT in environment variables or .env file.")
        return

    print("[*] Resolving channel...")
    guild_id, channel_id, channel_name = parse_room_input(room_input, token)
    print(f"[+] Target Channel: {channel_name} ({channel_id})")

    streamer = DiscordStreamClient(token, guild_id, channel_id, role="streamer")
    viewer = DiscordStreamClient(viewer_token, guild_id, channel_id, role="viewer") if viewer_token else None

    tasks = [asyncio.create_task(streamer.run_forever())]
    if viewer:
        tasks.append(asyncio.create_task(viewer.run_forever()))

    if viewer:
        try:
            await asyncio.wait_for(streamer.stream_created_event.wait(), timeout=15.0)
            await streamer.ws.send_json({"op": 20, "d": {"stream_key": streamer.stream_key}})
        except Exception:
            pass

    print("[*] Bot/Streamer is running continuously in background...")
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
