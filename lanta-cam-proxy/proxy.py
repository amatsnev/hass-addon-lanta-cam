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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lanta-proxy")

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

BASE_URL = "https://cam.lanta-net.ru"
LOGIN = os.environ.get("LANTA_LOGIN", "")
PASSWORD = os.environ.get("LANTA_PASSWORD", "")
TOKEN_REFRESH_INTERVAL = int(os.environ.get("TOKEN_REFRESH_INTERVAL", "7200"))
PORT = int(os.environ.get("PORT", "8080"))

cameras = {}
last_refresh = 0
_stream_session = None


def calc_password_hash(login, password):
    phone = "".join(c for c in login if c.isdigit())
    return hashlib.md5((phone + password).encode()).hexdigest()


async def get_stream_session():
    global _stream_session
    if _stream_session and not _stream_session.closed:
        return _stream_session
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    _stream_session = aiohttp.ClientSession(
        connector=connector,
        headers={
            "Referer": "https://cam.lanta-net.ru/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
    )
    return _stream_session


async def refresh_cameras():
    global cameras, last_refresh
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(BASE_URL, cookies=jar, connector=connector) as session:
        try:
            async with session.post("/user/gettoken") as resp:
                token = await resp.text()
                if resp.status != 200:
                    log.error("Failed to get token: %s %s", resp.status, token)
                    return

            pwd_hash = calc_password_hash(LOGIN, PASSWORD)
            async with session.post("/user/auth", data={
                "login": LOGIN, "password": pwd_hash, "token": token,
            }) as resp:
                redirect = (await resp.text()).strip()
                if resp.status != 200 or not redirect.startswith("/"):
                    log.error("Auth failed: %s %s", resp.status, redirect)
                    return

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
                url_path, _, token_qs = item["link_video"].partition("?")
                cameras[cam_id] = {
                    "title": item["title"],
                    "base_url": url_path.rsplit("/", 1)[0],
                    "token_qs": token_qs,
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


def rewrite_m3u8(text, cam_id, upstream_base, proxy_base):
    text = text.replace(upstream_base, proxy_base)
    text = re.sub(r'\?token=[^"]+', '', text)
    return text


async def proxy_stream(request):
    cam_id = int(request.match_info["cam_id"])
    path = request.match_info.get("path", "index.fmp4.m3u8")

    cam = cameras.get(cam_id)
    if not cam:
        return web.Response(status=404, text="Camera not found")

    upstream_url = f"{cam['base_url']}/{path}?{cam['token_qs']}"
    proxy_base = f"/stream/{cam_id}"

    try:
        session = await get_stream_session()
        async with session.get(upstream_url) as resp:
            content = await resp.read()
            content_type = resp.headers.get("Content-Type", "application/vnd.apple.mpegurl")

            if content_type and "mpegurl" in content_type or path.endswith(".m3u8"):
                text = content.decode("utf-8", errors="replace")
                text = rewrite_m3u8(text, cam_id, cam["base_url"], proxy_base)
                content = text.encode("utf-8")

            headers = {"Content-Type": content_type, "Access-Control-Allow-Origin": "*"}
            return web.Response(body=content, status=resp.status, headers=headers)
    except Exception as e:
        log.error("Proxy error for camera %s path %s: %s", cam_id, path, e)
        return web.Response(status=502, text=f"Upstream error: {e}")


async def list_cameras(request):
    host = request.headers.get("Host", f"0.0.0.0:{PORT}")
    scheme = "http"
    result = []
    for cam_id, cam in cameras.items():
        result.append({
            "id": cam_id,
            "title": cam["title"],
            "stream": f"{scheme}://{host}/stream/{cam_id}/index.fmp4.m3u8",
            "preview": f"{scheme}://{host}/preview/{cam_id}",
        })
    return web.json_response({"cameras": result, "last_refresh": last_refresh})


async def proxy_preview(request):
    cam_id = int(request.match_info["cam_id"])
    cam = cameras.get(cam_id)
    if not cam:
        return web.Response(status=404, text="Camera not found")

    try:
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        jar = aiohttp.CookieJar()
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession(BASE_URL, connector=connector, cookies=jar, headers=headers) as session:
            token = await (await session.post("/user/gettoken")).text()
            pwd_hash = calc_password_hash(LOGIN, PASSWORD)
            await session.post("/user/auth", data={
                "login": LOGIN, "password": pwd_hash, "token": token,
            })
            async with session.get(cam["preview_url"]) as resp:
                content = await resp.read()
                return web.Response(body=content, content_type=resp.headers.get("Content-Type", "image/jpeg"))
    except Exception as e:
        log.error("Preview error: %s", e)
        return web.Response(status=502, text=str(e))


async def force_refresh(request):
    await refresh_cameras()
    return web.json_response({"status": "ok", "cameras": len(cameras)})


app = web.Application()
app.router.add_get("/", list_cameras)
app.router.add_get("/cameras", list_cameras)
app.router.add_get("/stream/{cam_id}/{path:.+}", proxy_stream)
app.router.add_get("/preview/{cam_id}", proxy_preview)
app.router.add_post("/refresh", force_refresh)

async def on_startup(app):
    app["refresh_task"] = asyncio.create_task(background_refresh())

async def on_cleanup(app):
    global _stream_session
    if _stream_session and not _stream_session.closed:
        await _stream_session.close()

app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

if __name__ == "__main__":
    log.info("Starting Lanta Camera Proxy on port %d", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)
