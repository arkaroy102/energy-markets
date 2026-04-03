FROM python:latest

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "ercot_api.py"]