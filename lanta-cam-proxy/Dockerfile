FROM homeassistant/amd64-base:latest

RUN apk add --no-cache python3 py3-pip jq && \
    pip3 install --no-cache-dir --break-system-packages aiohttp

COPY proxy.py /proxy.py

ENV PORT=8199
EXPOSE 8199

CMD ["python3", "/proxy.py"]
