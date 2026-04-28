FROM python:3.13-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN chmod +x entrypoint.sh
CMD ["./entrypoint.sh"]