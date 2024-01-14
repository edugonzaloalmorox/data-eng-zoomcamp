FROM python:3.10

RUN apt-get install wget
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
RUN pip install pyarrow


WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]