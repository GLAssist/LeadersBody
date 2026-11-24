FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Kolkata

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Leaders15MinVNBB.py scripts.json .

RUN useradd -m app
USER app

CMD ["python", "Leaders15MinVNBB.py"]