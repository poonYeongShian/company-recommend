CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number text PRIMARY KEY,
    name                text,
    description         text,
    industry            text,
    state               text,
    status              text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'struck_off')),
    -- Source timestamps (from the feed's updatedAt, not arrival time): they decide which value wins.
    -- field_updated_at maps each of name, description, industry, state to the timestamp of its current value.
    field_updated_at    jsonb NOT NULL DEFAULT '{}',
    status_updated_at   timestamptz NOT NULL,
    -- The exact text the embedding was computed from, so we only re-embed when it changes.
    embedded_text       text,
    embedding           vector(384),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS companies_embedding_hnsw ON companies USING hnsw (embedding vector_cosine_ops);

-- Messages that could not be applied, kept for inspection and replay. message_id is the AMQP id (or a body hash).
CREATE TABLE IF NOT EXISTS dead_letters (
    message_id  text PRIMARY KEY,
    reason      text NOT NULL,
    body        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
