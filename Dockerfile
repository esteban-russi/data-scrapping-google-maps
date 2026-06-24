FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.py main.py ./
COPY data/responses.csv data/
CMD ["python", "main.py"]
