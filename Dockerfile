FROM nvidia/cuda:12.8.0-base-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY . .

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

EXPOSE 8080

CMD ["python3", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080", "--reload", "--workers", "2"]