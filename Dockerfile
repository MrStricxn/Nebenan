FROM python:3.11-slim

# System deps for Playwright Chromium + Xvfb (needed since nebenan.de blocks headless)
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libdbus-1-3 libxcb1 libxkbcommon0 libx11-6 libxcomposite1 \
    libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libwayland-client0 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

# Persistent dirs — Railway volume should be mounted at /app/data
RUN mkdir -p data cookies logs

EXPOSE 8000

# Start virtual display then run app (sh-compatible redirect).
# `exec` replaces sh with python so SIGTERM reaches uvicorn (graceful stop).
CMD sh -c "Xvfb :99 -screen 0 1280x1024x24 -ac +extension GLX +render -noreset > /dev/null 2>&1 & sleep 1 && exec DISPLAY=:99 python web.py"
