import asyncio
import hashlib
import logging
import os
import re
import ssl
import time
from urllib.parse import urljoin

import aiohttp
from aiohttp import web

FFMPEG = os.environ.get("FFMPEG_PATH", "ffmpeg")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lanta-proxy")

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

BASE_URL = "https://cam.lanta-net.ru"
LOGIN = os.environ.get("LANTA_LOGIN", "")
PASSWORD = os.environ.get("LANTA_PASSWORD", "")
TOKEN_REFRESH_INTERVAL = int(os.environ.get("TOKEN_REFRESH_INTERVAL", "5400"))
PORT = int(os.environ.get("PORT", "8080"))

cameras = {}
last_refresh = 0
_auth_session = None
_auth_lock = asyncio.Lock()
_auth_time = 0


def calc_password_hash(login, password):
    phone = "".join(c for c in login if c.isdigit())
    return hashlib.md5((phone + password).encode()).hexdigest()


async def get_auth_session():
    global _auth_session, _auth_time
    async with _auth_lock:
        if _auth_session and not _auth_session.closed and (time.time() - _auth_time) < TOKEN_REFRESH_INTERVAL:
            return _auth_session
        if _auth_session and not _auth_session.closed:
            await _auth_session.close()
        jar = aiohttp.CookieJar()
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        session = aiohttp.ClientSession(
            BASE_URL, cookies=jar, connector=connector,
            headers={
                "Referer": "https://cam.lanta-net.ru/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
        )
        try:
            token = await (await session.post("/user/gettoken")).text()
            pwd_hash = calc_password_hash(LOGIN, PASSWORD)
            async with session.post("/user/auth", data={
                "login": LOGIN, "password": pwd_hash, "token": token,
            }) as resp:
                redirect = (await resp.text()).strip()
                if resp.status != 200 or not redirect.startswith("/"):
                    log.error("Auth failed: %s %s", resp.status, redirect)
                    await session.close()
                    return None
            _auth_session = session
            _auth_time = time.time()
            log.info("Auth session created/refreshed")
            return session
        except Exception as e:
            log.error("Auth error: %s", e)
            await session.close()
            return None


async def refresh_cameras():
    global cameras, last_refresh
    session = await get_auth_session()
    if not session:
        return
    try:
        async with session.get("/cameras/get", params={
            "page": 1, "limit": 100, "search": ""
        }) as resp:
            data = await resp.json()
        if not data.get("items"):
            log.warning("No cameras found")
            return
        old_count = len(cameras)
        cameras.clear()
        for item in data["items"]:
            cam_id = item["id"]
            cameras[cam_id] = {
                "title": item["title"],
                "link_video": item["link_video"],
                "preview_url": urljoin(BASE_URL, item["link_preview"]),
            }
            log.info("Camera %s: %s", item["title"], item["id"])
        last_refresh = time.time()
        log.info("Refreshed %d cameras (was %d)", len(cameras), old_count)
    except Exception as e:
        log.error("Error refreshing cameras: %s", e)


async def background_refresh():
    while True:
        await refresh_cameras()
        await asyncio.sleep(TOKEN_REFRESH_INTERVAL)


async def proxy_stream(request):
    cam_id = int(request.match_info["cam_id"])
    path = request.match_info.get("path", "index.fmp4.m3u8")

    cam = cameras.get(cam_id)
    if not cam:
        return web.Response(status=404, text="Camera not found")

    full_url = cam["link_video"]
    url_path, _, token_qs = full_url.partition("?")
    base_url = url_path.rsplit("/", 1)[0]
    upstream_url = f"{base_url}/{path}?{token_qs}" if token_qs else f"{base_url}/{path}"

    session = await get_auth_session()
    if not session:
        return web.Response(status=502, text="Auth failed")

    try:
        async with session.get(upstream_url) as resp:
            if resp.status == 403:
                global _auth_session, _auth_time
                async with _auth_lock:
                    if _auth_session and not _auth_session.closed:
                        await _auth_session.close()
                    _auth_session = None
                    _auth_time = 0
                session = await get_auth_session()
                if not session:
                    return web.Response(status=502, text="Re-auth failed")
                async with session.get(upstream_url) as resp:
                    return await _process_stream_response(resp, cam_id, path, base_url)
            return await _process_stream_response(resp, cam_id, path, base_url)
    except Exception as e:
        log.error("Proxy error for camera %s path %s: %s", cam_id, path, e)
        return web.Response(status=502, text=f"Upstream error: {e}")


async def _process_stream_response(resp, cam_id, path, upstream_base):
    log.info("Upstream %s for cam %s path %s", resp.status, cam_id, path)
    content = await resp.read()
    if resp.status != 200 and not path.endswith(".m3u8"):
        log.error("Upstream error %s for cam %s path %s: %s", resp.status, cam_id, path, content[:200])
        return web.Response(status=resp.status, body=content)
    content_type = resp.headers.get("Content-Type", "application/vnd.apple.mpegurl")

    if (content_type and "mpegurl" in content_type) or path.endswith(".m3u8"):
        text = content.decode("utf-8", errors="replace")
        proxy_base = f"/stream/{cam_id}"
        text = text.replace(upstream_base, proxy_base)
        text = re.sub(r'\?token=[^\s"]+', '', text)
        content = text.encode("utf-8")

    headers = {"Content-Type": content_type, "Access-Control-Allow-Origin": "*"}
    return web.Response(body=content, status=resp.status, headers=headers)


async def list_cameras(request):
    host = request.headers.get("Host", f"0.0.0.0:{PORT}")
    scheme = "http"
    result = []
    for cam_id, cam in cameras.items():
        result.append({
            "id": cam_id,
            "title": cam["title"],
            "stream": f"{scheme}://{host}/stream/{cam_id}/index.fmp4.m3u8",
            "ts": f"{scheme}://{host}/ts/{cam_id}",
            "preview": f"{scheme}://{host}/preview/{cam_id}",
        })
    return web.json_response({"cameras": result, "last_refresh": last_refresh})


async def proxy_preview(request):
    cam_id = int(request.match_info["cam_id"])
    cam = cameras.get(cam_id)
    if not cam:
        return web.Response(status=404, text="Camera not found")

    session = await get_auth_session()
    if not session:
        return web.Response(status=502, text="Auth failed")

    try:
        async with session.get(cam["preview_url"]) as resp:
            content = await resp.read()
            return web.Response(body=content, content_type=resp.headers.get("Content-Type", "image/jpeg"))
    except Exception as e:
        log.error("Preview error: %s", e)
        return web.Response(status=502, text=str(e))


async def proxy_ts_stream(request):
    cam_id = int(request.match_info["cam_id"])
    cam = cameras.get(cam_id)
    if not cam:
        return web.Response(status=404, text="Camera not found")

    session = await get_auth_session()
    if not session:
        return web.Response(status=502, text="Auth failed")

    hls_url = cam["link_video"]

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "video/mp2t",
            "Cache-Control": "no-cache",
            "Access-Control-Allow-Origin": "*",
        },
    )
    await response.prepare(request)

    proc = None
    try:
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "warning",
            "-fflags", "nobuffer+genpts",
            "-flags", "low_delay",
            "-analyzeduration", "1000000",
            "-probesize", "500000",
            "-i", hls_url,
            "-c", "copy",
            "-f", "mpegts",
            "-mpegts_service_type", "digital_tv",
            "-flush_packets", "1",
            "-",
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log.info("ffmpeg started for cam %s (ts)", cam_id)

        async def _read_stderr():
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                log.warning("ffmpeg[%s]: %s", cam_id, line.decode(errors="replace").rstrip())

        stderr_task = asyncio.create_task(_read_stderr())

        try:
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                try:
                    await response.write(chunk)
                except (ConnectionResetError, ConnectionError):
                    break
        finally:
            stderr_task.cancel()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error("TS stream error for cam %s: %s", cam_id, e)
    finally:
        if proc and proc.returncode is None:
            proc.kill()
            await proc.wait()
            log.info("ffmpeg stopped for cam %s (ts)", cam_id)

    return response


async def force_refresh(request):
    global _auth_session, _auth_time
    async with _auth_lock:
        if _auth_session and not _auth_session.closed:
            await _auth_session.close()
        _auth_session = None
        _auth_time = 0
    await refresh_cameras()
    return web.json_response({"status": "ok", "cameras": len(cameras)})


app = web.Application()
app.router.add_get("/", list_cameras)
app.router.add_get("/cameras", list_cameras)
app.router.add_get("/stream/{cam_id}/{path:.+}", proxy_stream)
app.router.add_get("/ts/{cam_id}", proxy_ts_stream)
app.router.add_get("/preview/{cam_id}", proxy_preview)
app.router.add_post("/refresh", force_refresh)

async def on_startup(app):
    app["refresh_task"] = asyncio.create_task(background_refresh())

async def on_cleanup(app):
    global _auth_session
    if _auth_session and not _auth_session.closed:
        await _auth_session.close()

app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    log.info("Starting Lanta Camera Proxy on port %d", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)
