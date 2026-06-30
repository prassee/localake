FROM postgres:14

RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && rm -rf /var/lib/apt/lists/*

COPY ./postgres/conf/pg_hba.conf /etc/postgresql/pg_hba.conf
COPY ./postgres/initdb/01-olake-logical-replication.sql /docker-entrypoint-initdb.d/01-olake-logical-replication.sql

CMD ["postgres"]
