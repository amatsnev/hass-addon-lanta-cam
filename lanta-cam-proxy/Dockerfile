ARG BUILD_ARCH
FROM homeassistant/${BUILD_ARCH}-base:latest

RUN apk add --no-cache python3 py3-pip jq && \
    pip3 install --no-cache-dir --break-system-packages aiohttp

COPY rootfs /
COPY proxy.py /proxy.py
RUN chmod +x /etc/services.d/lanta-cam-proxy/run

CMD ["/etc/services.d/lanta-cam-proxy/run"]
