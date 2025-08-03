create table if not exists song_play_agg (
  window_start timestamp,
  window_end timestamp,
  song_id bigint not null,
  play_count bigint not null,
  primary key (window_start, song_id)
);
