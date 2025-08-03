import os

from pyflink.table import (
    EnvironmentSettings, TableEnvironment
)


def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = os.getenv("KAFKA_TOPIC")

    if kafka_bootstrap_servers is None:
        raise ValueError("Kafka bootstrap servers parameter is required")
    if kafka_topic is None:
        raise ValueError("Kafka topic parameter is required")

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.execute_sql(f"""
    CREATE TABLE song_plays (
        event_id STRING,
        song_id INT,
        duration DOUBLE,
        event_ts STRING,
        event_time AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTES
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{kafka_topic}',
        'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
        'properties.group.id' = 'pyflink-sql-consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """)

    result = t_env.sql_query("""
    WITH daily_counts AS (
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' DAY) AS play_day,
            song_id,
            COUNT(*) AS play_count
        FROM song_plays
        GROUP BY
            TUMBLE(event_time, INTERVAL '1' DAY),
            song_id
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_day ORDER BY play_count DESC) AS rownum
        FROM daily_counts
    )
    SELECT play_day, song_id, play_count
    FROM ranked
    WHERE rownum <= 100""")
    result.execute().print()


if __name__ == "__main__":
    main()
