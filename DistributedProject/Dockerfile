FROM python:3.9-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY node.py .
COPY monitor.py .
CMD ["python", "node.py"]
