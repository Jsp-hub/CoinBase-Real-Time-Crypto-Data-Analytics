FROM python:3.12-slim

WORKDIR /opt/app

COPY services/analytics_writer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/analytics_writer/candle_to_mysql_writer.py candle_to_mysql_writer.py

CMD ["python", "candle_to_mysql_writer.py"]
