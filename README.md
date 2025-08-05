# 🎧 Top-K Analytics

This project simulates a real-time analytics pipeline that computes and visualizes the top-K most played songs over a custom period (e.g., last 24 hours, last 7 days, last month).

While originally inspired by the *“Design Spotify Top K Songs”* system design problem, the concept generalizes to any top-K use case, such as most viewed videos, most read articles, or most purchased products.

## 📦 Components

- **`event-generator/`** – Produces synthetic song play events and streams them into a Kafka topic.
- **`stream-processor/`** – A PyFlink application that consumes, aggregates, and stores results in a PostgreSQL-based datamart.
- **`dashboard/`** – *[TODO]* A Metabase dashboard to visualize top-K songs over a user-defined time range.