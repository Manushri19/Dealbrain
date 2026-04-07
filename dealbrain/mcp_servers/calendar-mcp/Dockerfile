FROM python:3.14-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY calendar_server.py .
EXPOSE 8080
CMD ["uvicorn", "calendar_server:app", "--host", "0.0.0.0", "--port", "8080"]
