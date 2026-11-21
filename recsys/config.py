"""Connection settings. Defaults match docker-compose.yml; override with environment variables.

The hosts are 127.0.0.1, not localhost: compose publishes the ports on IPv4 only, and on Windows "localhost" tries
IPv6 first, which can stall instead of failing fast.
"""

import os


def database_url() -> str:
    port = os.environ.get("POSTGRES_PORT", "5432")
    return os.environ.get("DATABASE_URL", f"postgresql://postgres:postgres@127.0.0.1:{port}/companies")


def rabbitmq_url() -> str:
    port = os.environ.get("RABBITMQ_PORT", "5672")
    return os.environ.get("RABBITMQ_URL", f"amqp://guest:guest@127.0.0.1:{port}/")
