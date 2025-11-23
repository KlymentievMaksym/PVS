FROM python:3.13

WORKDIR /app

COPY req.txt .
RUN pip install --no-cache-dir -r req.txt

COPY ./Updates ./Updates
COPY PostgreUpdates.py .

CMD ["python", "PostgreUpdates.py"]