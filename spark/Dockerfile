FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir psycopg2-binary

COPY jars/ /opt/spark/jars/
COPY spark/streaming_job.py /app/streaming_job.py

WORKDIR /app

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "streaming_job.py"]
