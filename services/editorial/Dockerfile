FROM python:3.11-slim-bookworm

# 1. Install System Dependencies
# 'curl' and 'build-essential' for python libs
# Playwright dependencies will be installed by the 'playwright install-deps' command later
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup Workdir
WORKDIR /app

# 3. Install Common Lib (Local Dependency)
# We copy the common folder first to leverage Docker cache
COPY common /app/common
RUN pip install --no-cache-dir -e /app/common

# 4. Install Editorial Dependencies
COPY services/editorial/requirements.txt /app/services/editorial/requirements.txt
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r /app/services/editorial/requirements.txt

# 5. Install Playwright Browsers (Critical for Harvester)
# This installs Chromium and its OS-level dependencies
RUN playwright install chromium
RUN playwright install-deps chromium
COPY services/editorial/scripts/download.py /app/services/editorial/scripts/download.py
RUN python services/editorial/scripts/download.py


RUN python -m spacy download pt_core_news_lg

# 6. Copy Codebase
COPY services/editorial /app/services/editorial




# 7. Set Python Path so workers can find 'core', 'domain', etc.
ENV PYTHONPATH=/app/services/editorial
