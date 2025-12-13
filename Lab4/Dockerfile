FROM python:3.13

WORKDIR /app

COPY req.txt .
RUN pip install --no-cache-dir -r req.txt

COPY main.py .

CMD ["python", "main.py"]