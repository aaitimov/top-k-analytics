FROM apache/flink:1.17.2

RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

RUN curl -L -o /opt/flink/lib/flink-connector-jdbc-3.1.2-1.17.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar

RUN curl -L -o /opt/flink/lib/postgresql-42.7.3.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt-get update -y && apt-get install -y python3 python3-pip python3-dev

COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/

COPY pyproject.toml uv.lock /app/

WORKDIR /app

RUN uv sync --locked

COPY app/ app/

ENV PATH="/app/.venv/bin:$PATH"
