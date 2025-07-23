FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY IngestingDataToPostgres.py IngestingDataToPostgres.py

ENTRYPOINT [ "python", "IngestingDataToPostgres.py" ]