from pydantic_settings import BaseSettings
from pyflink.table import EnvironmentSettings, TableEnvironment


class AppSettings(BaseSettings):
    kafka_bootstrap_servers: str
    kafka_topic: str
    db_host: str
    db_port: int
    db_schema: str
    db_user: str
    db_password: str


def main():
    app_settings = AppSettings()  # pyright: ignore[reportCallIssue]

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql(
        f"""
        create table song_plays (
          event_id string,
          song_id int,
          duration double,
          event_ts string,
          event_time AS to_timestamp(event_ts, 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
          watermark for event_time as event_time - interval '1' minute
        ) with (
          'connector' = 'kafka',
          'topic' = '{app_settings.kafka_topic}',
          'properties.bootstrap.servers' = '{app_settings.kafka_bootstrap_servers}',
          'properties.group.id' = 'pyflink-sql-consumer',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true'
        )
        """
    )

    jdbc_url = (
        f"jdbc:postgresql://"
        f"{app_settings.db_host}:{app_settings.db_port}/{app_settings.db_schema}"
    )
    t_env.execute_sql(
        f"""
        create table song_play_agg (
          window_start timestamp,
          window_end timestamp,
          song_id bigint,
          play_count bigint,
          primary key (window_start, song_id) not enforced
        ) with (
          'connector' = 'jdbc',
          'url' = '{jdbc_url}',
          'table-name' = 'song_play_agg',
          'username' = '{app_settings.db_user}',
          'password' = '{app_settings.db_password}',
          'driver' = 'org.postgresql.Driver'
        )
        """
    )

    result = t_env.sql_query(
        """
        select
          tumble_start(event_time, interval '5' minutes) as window_start,
          tumble_end(event_time, interval '5' minutes) as window_end,
          song_id,
          count(*) as play_count
        from song_plays
        group by
          tumble(event_time, interval '5' minutes),
          song_id"""
    )
    result.execute_insert("song_play_agg")


if __name__ == "__main__":
    main()
