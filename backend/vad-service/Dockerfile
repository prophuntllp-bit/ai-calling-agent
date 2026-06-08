FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app:/app/..
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download Silero VAD model at build time so the container starts
# without needing network access to GitHub on every restart.
RUN python -c "\
import torch; \
torch.hub.load('snakers4/silero-vad', 'silero_vad', force_reload=False, trust_repo=True); \
print('Silero VAD model cached.')"
COPY . .
EXPOSE 8001
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8001}"]
