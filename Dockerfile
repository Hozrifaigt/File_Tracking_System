FROM python:3.12-slim-bookworm


WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
EXPOSE 8080
COPY . .
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080", "--reload", "--workers", "2"]