set shell := ['bash', '-cu']

DOCKER_COMPOSE := "docker compose"
COMPOSE_FILE := "docker-compose.yaml"

# Default task
default: up

up:
    @echo "Starting services (detached)..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} up -d

down:
    @echo "Stopping and removing containers, networks and volumes..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} down --volumes --remove-orphans

start:
    @echo "Starting existing containers..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} start

stop:
    @echo "Stopping containers..."
    @{{DOCKER_COMPOSE}} -f {{COMPOSE_FILE}} stop

restart:
    @just down
    @just up

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
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar /opt/etl/src/compact_partition.py 2026-05-01 2026-05-14"

pyspark-stream-upsert-submit:
    docker exec -it spark-master /bin/bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/etl/lib/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/etl/lib/hadoop-aws-3.3.4.jar,/opt/etl/lib/aws-java-sdk-bundle-1.12.603.jar,/opt/etl/lib/hadoop-common-3.3.4.jar /opt/etl/src/stream_upsert_heimdall.py"



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
