FROM pgvector/pgvector:pg18

RUN apt-get update && \
    apt-get install -y postgresql-18-cron

COPY services/db/db-init.sh /docker-entrypoint-initdb.d
