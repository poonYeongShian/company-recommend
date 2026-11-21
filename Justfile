set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Run the consumer until stopped. Pass `--idle-exit 5` to stop once the queue is empty.
consumer *args:
    uv run python -m recsys.consumer {{args}}

# Run the "Similar companies" API on http://127.0.0.1:8000 (docs at /docs).
serve *args:
    uv run python -m recsys.api {{args}}

# Run the tests (the integration tests need `just up`).
test:
    uv run pytest
