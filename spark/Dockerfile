FROM apache/spark:3.5.3

USER root

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark/.ivy2 && \
    pip install psycopg2-binary

USER spark