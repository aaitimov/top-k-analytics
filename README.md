# Top-K Analytics

This project simulates a real-time analytics pipeline that computes and visualizes the top-K most played songs.

## Components

- **event-generator/**: Produces dummy song playback events into a Kafka topic.
- **stream-processor/**: A Flink app that consumes playback events, aggregates them, enriches them with metadata, and stores results in a database.
- **dashboard/**: Contains Superset dashboards or setup instructions for visualizing top-K results.
