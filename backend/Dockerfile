FROM python:latest

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]