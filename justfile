set shell := ['bash', '-cu']

DOCKER_COMPOSE := "docker compose"
COMPOSE_FILE := "docker-compose.yaml"

# Default task
default: up

up:
    @echo "Starting services (detached)..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} --profile debug up -d

down:
    @echo "Stopping and removing containers, networks and volumes..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} --profile debug down --volumes --remove-orphans

start:
    @echo "Starting existing containers..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} start

stop:
    @echo "Stopping containers..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} stop

restart:
    @just down
    @just up

olake-up:
    @echo "Starting Olake and dependent services..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} --profile debug up -d postgres minio nessie olake-postgres temporal temporal-ui olake-ui olake-worker

olake-down:
    @echo "Stopping Olake services..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} stop olake-ui olake-worker temporal temporal-ui olake-postgres postgres minio nessie || true

olake-logs:
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} logs -f --tail 200 olake-ui olake-worker temporal temporal-ui olake-postgres postgres

all-up:
    @echo "Starting the full localake stack..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} up -d

ps:
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} ps

logs service='':
    @if [ -z "{{service}}" ]; then \
        {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} logs -f --tail 200; \
    else \
        {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} logs -f --tail 200 {{service}}; \
    fi

build:
    @echo "Building images (if Dockerfile present)..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} build --pull

exec service cmd='sh':
    @echo "Execing into {{service}} (cmd='{{cmd}}')"
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec {{service}} {{cmd}}

compose args='':
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} {{args}}

MAVEN := "https://repo1.maven.org/maven2"
LIB_DIR := "etl/lib"

# Download all Spark/Iceberg/Nessie jars into etl/lib/
download-jars:
    @mkdir -p {{LIB_DIR}}
    @for jar_url in \
        "{{MAVEN}}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar" \
        "{{MAVEN}}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
        "{{MAVEN}}/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar" \
        "{{MAVEN}}/com/amazonaws/aws-java-sdk-bundle/1.12.603/aws-java-sdk-bundle-1.12.603.jar" \
        "{{MAVEN}}/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.1_2.12/0.59.0/nessie-spark-extensions-3.1_2.12-0.59.0.jar" \
    ; do \
        filename=$(basename "$jar_url"); \
        dest="{{LIB_DIR}}/$filename"; \
        if [ -f "$dest" ]; then \
            echo "Already exists: $dest"; \
        else \
            echo "Downloading $filename ..."; \
            curl -fsSL "$jar_url" -o "$dest" && echo "  -> $dest" || { echo "FAILED: $jar_url"; exit 1; }; \
        fi; \
    done
    @echo "All jars ready in {{LIB_DIR}}/"

generate-upi-events:
    cd etl && uv run python -c "from src.generate_faker_csv import generate_upi_events; generate_upi_events()"

pyspark-submit:
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar /opt/etl/src/create_iceberg_table.py"

pyspark-small-file-submit:
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar /opt/etl/src/small_files_table.py"

pyspark-load-upi-submit:
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar,/opt/etl/lib/nessie-spark-extensions-3.1_2.12-0.59.0.jar /opt/etl/src/load_upi_events.py"

pyspark-compact-submit:
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar /opt/etl/src/compact_partition.py 2025-06-01 2026-06-28"

pyspark-stream-upi-submit trigger_seconds='61' max_files='500' merge_keys='id' latest_ts_col='_olake_timestamp':
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar,/opt/etl/lib/nessie-spark-extensions-3.1_2.12-0.59.0.jar /opt/etl/src/stream_s3_to_iceberg.py --source-path s3://stage/heimdall/heimdall/upi_transactions/2026-06-30/03 --table nessie.heimdall_1_sync_nessie_public.upi_transactions_v2 --checkpoint s3a://stage/checkpoints/stream_upi_transactions --trigger-seconds {{trigger_seconds}} --max-files-per-trigger {{max_files}} --merge-keys {{merge_keys}} --latest-timestamp-col {{latest_ts_col}}"

pyspark-backfill-upi-submit merge_keys='id' latest_ts_col='_olake_timestamp':
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar,/opt/etl/lib/nessie-spark-extensions-3.1_2.12-0.59.0.jar /opt/etl/src/backfill_upi_to_iceberg.py --source-path s3://stage/heimdall/heimdall/upi_transactions --table nessie.heimdall_1_sync_nessie_public.upi_transactions_v2 --merge-keys {{merge_keys}} --latest-timestamp-col {{latest_ts_col}}"

# Classpath for running inside spark-master: our class + iceberg/aws jars + Spark's
# bundled hadoop-client jars (which carry all the S3A transitive deps).
JAVA_RUN_CP := "/opt/etl/lib/classes:/opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar:/opt/etl/lib/hadoop-aws-3.3.4.jar:/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar:/opt/spark/jars/*"
MINIO_MC := "docker run --rm --network localake_localake_net --entrypoint /bin/sh minio/mc"

# Compile the standalone Iceberg equality-delete writer (no Spark) against the lab jars.
# Output lands in etl/lib/classes, which is mounted into the Spark containers.
java-eq-build:
    docker run --rm -v "$PWD":/work -w /work eclipse-temurin:11-jdk \
        bash -c "mkdir -p etl/lib/classes && javac -cp 'etl/lib/*' -d etl/lib/classes javawriter/HeimdallEqWriter.java && echo COMPILE_OK"

# Run the equality-delete upsert into nessie.db.heimdall. Usage: just java-eq-run [max_files]
java-eq-run max_files='50':
    docker exec \
        -e NESSIE_URI=http://nessie:19120/api/v1 \
        -e S3_ENDPOINT=http://minio:9000 \
        -e WAREHOUSE=s3a://warehouse/ \
        -e MAX_FILES={{max_files}} \
        spark-master java -cp "{{JAVA_RUN_CP}}" HeimdallEqWriter

# Drop a table from nessie.heimdall and clear only that table's MinIO objects.
# Usage: just heimdall-drop-table [table]
heimdall-drop-table table='upi_transactions':
    @echo "Dropping nessie.heimdall.{{table}} and clearing matching MinIO objects..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec -T trino trino --execute "DROP TABLE IF EXISTS nessie.heimdall.{{table}}"
    @{{MINIO_MC}} -c "mc alias set local http://minio:9000 minio minio123 >/dev/null && for path in $$(mc find local/warehouse/heimdall --name '{{table}}_*' 2>/dev/null); do mc rm --recursive --force \"$$path\"; done"

# Drop the entire nessie.heimdall schema and reclaim all MinIO objects under it.
# Usage: just heimdall-drop-schema
heimdall-drop-schema:
    @echo "Dropping nessie.heimdall schema and clearing its MinIO prefix..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec -T trino trino --execute "DROP SCHEMA IF EXISTS nessie.heimdall CASCADE"
    @{{MINIO_MC}} -c "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc rm --recursive --force local/warehouse/heimdall || true"

# Drop upi tables from nessie.heimdall_1_sync_nessie_public and clear matching MinIO objects.
# Usage: just heimdall-sync-drop-upi-tables
heimdall-sync-drop-upi-tables:
    @echo "Dropping nessie.heimdall_1_sync_nessie_public.upi_transactions and upi_transactions_v2..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec -T trino trino --execute "DROP TABLE IF EXISTS nessie.heimdall_1_sync_nessie_public.upi_transactions"
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec -T trino trino --execute "DROP TABLE IF EXISTS nessie.heimdall_1_sync_nessie_public.upi_transactions_v2"
    @{{MINIO_MC}} -c 'mc alias set local http://minio:9000 minio minio123 >/dev/null && mc find local/warehouse/heimdall_1_sync_nessie_public --name "upi_transactions*" 2>/dev/null | while IFS= read -r path; do mc rm --recursive --force "$path"; done'



help:
    @echo "Available targets: up down start stop restart ps logs build exec compose help"

# Generic service helpers to avoid repeating recipes per service.
# Usage: `just service <name> up|down|logs|exec <cmd>`
service name action='up' arg='':
        @case "{{action}}" in \
            up)    {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} up -d {{name}} ;; \
            down)  {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} stop {{name}} || true; {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} rm -f {{name}} || true ;; \
            logs)  {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} logs -f --tail 200 {{name}} ;; \
            exec)  if [ -z "{{arg}}" ]; then echo "usage: just service <name> exec <cmd>"; exit 1; fi; {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} exec {{name}} {{arg}} ;; \
            ps)    {{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} ps {{name}} ;; \
            *) echo "usage: just service <name> [up|down|logs|exec <cmd>|ps]"; exit 1 ;; \
        esac

# Short alias: `just svc minio up` -> `just service minio up`
svc name action='up' arg='':
    @just service {{name}} {{action}} {{arg}}
