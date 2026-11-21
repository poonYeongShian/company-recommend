# Design

How to run everything is in the [README](README.md). This file covers what I decided, what I assumed, what changes at
10x volume, and what is left.

## What the feed actually contains

I profiled `data/feed.jsonl` before writing any code, because the registry team says it does not validate what it
sends. The 2,518 lines hold 2,000 companies, and these are the problems the code has to survive:

| In the feed | Count | What the code does |
|---|---|---|
| Truncated JSON, or a bare `null` | 5, 6 | Dead-letter |
| Registration number missing, or an integer (`119`) | 7, 3 | Dead-letter. I do not guess a `SYN-` number |
| Registration number lowercase or padded (`" syn-001497 "`) | dozens of values | Strip and upper-case. Without this there are 2,053 "companies" |
| `updatedAt` as epoch milliseconds / without a timezone / missing | 30 / 14 / 7 | Convert from UTC / assume +08:00 / dead-letter |
| `data` as a string (`companyName=Foo; state=Perak`) or missing | 4, 5 | Dead-letter |
| Exact duplicate lines | 68 | Harmless: applying an event twice changes nothing |
| Arrival order differs from `updatedAt` order | 65 companies | Source time decides, never arrival time |
| Partial `data` (`companyName` only) | 14 | Blank fields never erase known ones |
| Blank description in one event, good one in another | 9 companies | Same rule; a naive "latest wins" would erase it |
| Company that never has a usable description | 39 | Embedded from name and industry (see below) |
| `company-struck-off` (no `data`, and no later event for that company) | 8 | Flag the company, keep it |
| State in mixed case (`KUALA LUMPUR`) | many | Map to the 16 canonical names |
| Description of one sentence repeated 60 times | 7 | Collapse to one copy |
| About 30% of descriptions in Malay (crude keyword count) | ~700 | Multilingual model |
| Only 178 distinct descriptions across 2,000 companies | | Many exact ties (see Search) |

Of the 2,518 messages, 37 are dead-lettered, which is 32 distinct rows because the table is keyed by message id.

## Decisions

**Merging events: each field keeps its latest non-blank value by source time.** Delivery is at-least-once and
unordered, so the result must not depend on arrival order. Each field carries its own `updatedAt`
(`field_updated_at`), and an event only replaces a field it is newer for. My first version kept one timestamp per
company, and a test that tries every ordering of five events found the flaw: an old event filling a blank field claimed
the newest timestamp, so a middle-aged event arriving later was wrongly ignored. Per-field timestamps fix it, and the
rule is then order-independent and idempotent. Tests cover this, and also replay the real feed forwards, backwards,
shuffled and twice, and require identical tables each time. I use the event's own timestamp rather than the AMQP
`message_id` for deduplication, because `message_id` is a hash of the body: it cannot tell a stale update from a
fresh one.

**Consumer.** Batches of up to 100 messages, one transaction per batch, and messages are acked only after the commit,
so a crash redelivers them. If a batch fails unexpectedly it is retried one message at a time, so one poison message
cannot block the others. Bad messages are written to a `dead_letters` table and acked. I did not put a dead-letter
exchange on `company-events`, because the registry team owns that queue and declares it with no arguments. A database
outage requeues the batch and reconnects. Rows are locked with `SELECT … FOR UPDATE` while a batch merges them.

**Embedding model: `paraphrase-multilingual-MiniLM-L12-v2` (384 dimensions, CPU, through fastembed), not the suggested
`bge-small-en-v1.5`.** With about 30% of descriptions in Malay, an English-only model compares a Malay "kedai kopi"
poorly with an English "coffee shop". I measured it rather than assuming (see "Going deeper"): same-industry
precision@5 is 0.85 versus 0.71. Descriptions are embedded once per distinct text, and a company is re-embedded only
when the text it was embedded from changes (`embedded_text` records it).

**Companies with no description are embedded from `name. industry`.** The portal needs "no lost companies", and 39
companies would otherwise never appear in anyone's recommendations. Name and industry give them neighbours in the right
industry. The cost is weaker matches for those 39.

**Struck-off companies are kept and flagged, but never recommended.** This is a judgement call. The requirement says
every company in the feed should appear in other companies' recommendations, but recommending a company that no longer
exists is a worse product than leaving it out. Their own page still gets recommendations. Reversing the choice means
removing one `WHERE` clause in `search.py`. A later upsert re-activates a struck-off company (latest event wins).

**Schema.** One `companies` table keyed by registration number, plus `dead_letters`. Source timestamps are stored
because they decide which value wins. The consumer and the API both create the schema if it is missing, behind an
advisory lock, so start-up order does not matter.

**Search is an exact scan, done in Postgres with pgvector.** For one company the API reads its embedding, then
orders everyone else by cosine distance (`<=>`), filtering out itself, struck-off and unnamed placeholder rows. Ties
are common (one description is shared by 47 companies, so they have identical embeddings), so ties break on
registration number to make answers stable. `EXPLAIN` shows Postgres chooses a sequential scan and sort here, so the
HNSW index I created is **not used** at this size. It is there for the scale-up path below. Measured on this machine
over 200 random companies: p50 6 ms, p95 8 ms end to end.

**API.** `GET /companies/{registrationNumber}/similar?limit=`. It normalizes the registration number the same way the
consumer does, returns 404 for unknown companies, 422 for bad input, and 503 with `Retry-After` when the database is
down, so the portal can hide the panel instead of breaking the page. A short pool timeout (2 s) makes it fail fast
instead of hanging. Start-up never blocks on the database.

## Assumptions

- Timestamps without an offset are Malaysian time (+08:00). That is a guess: 2,428 timestamps in the feed say +08:00, 28 say
  `Z` (UTC), and the 14 with no offset have no way to tell us, so I went with the dominant zone.
- Epoch numbers over 1e11 are milliseconds. An `updatedAt` more than a day in the future is rejected, because one bad
  value would otherwise win every later comparison.
- A blank or missing field means "not provided", never "erase". The cost: a company can never clear a field through
  the feed.
- A struck-off notice is the latest event for the company's *status*, compared with all events by time, and does not
  touch its data. A notice for a company we have never seen creates a nameless placeholder, which is hidden from the API
  (404) and from recommendations until a real upsert arrives.
- Only the description decides similarity. State and name do not, so a coffee shop in Johor is as similar to one in
  Selangor as to another in Johor. The portal could re-rank by state if it wanted to.
- Dead letters are never retried automatically. They need a human or a registry re-send.

## At 10x the volume

1. **More than one consumer.** RabbitMQ will hand messages to several consumers, and the merge rule already tolerates any
   order. There is one gap: if two consumers both see a *new* company at once, the second upsert overwrites the first
   instead of merging. The fix is to insert with `ON CONFLICT DO NOTHING`, then reload the row with `FOR UPDATE` and
   merge. I have not built this, and the consumer assumes a single instance today.
2. **Embedding is the CPU cost.** It is already deduplicated per batch and cached in memory. Next steps: a text-hash
   cache table shared by consumers (the feed has few distinct descriptions), a separate embedding stage on its own
   queue, and ONNX threads or a GPU.
3. **Search.** The exact scan grows linearly. The planned change is an indexed top-N followed by a re-sort: take the
   200 nearest by index (`ORDER BY embedding <=> v LIMIT 200`, with `hnsw.ef_search` raised to match), then order those by
   (distance, registration number) and filter. This keeps stable ties except when more than 200 companies tie. pgvector
   0.8's iterative scan handles filters that remove many candidates. If that is still too slow, precompute each
   company's top-k when its description changes and serve it as a key lookup.
4. **API.** It is stateless, so run more copies behind a load balancer, keep pool size times copies under
   `max_connections`, add a read replica, and put a short-lived cache in front.
5. **Storage and model changes.** Embeddings are 1.5 KB per company, which is fine at 10x. Changing the model means
   re-embedding everyone, so I would add an `embedding_model` column and a backfill job first.
6. **Monitoring.** Queue depth and oldest-message age, consumer batch latency, dead-letter rate by reason, API p95 and
   5xx, the share of companies without an embedding, and drift in the similarity-score distribution.

## Going deeper: is the model choice actually better?

`just evaluate` runs `scripts/evaluate_models.py`. For each distinct description it finds the 5 nearest other
descriptions and counts how many share the registry's `industry`, as precision@5.

| Model | All | English | Malay |
|---|---|---|---|
| Random guess | 0.11 | | |
| `bge-small-en-v1.5` | 0.71 | 0.78 | 0.58 |
| `paraphrase-multilingual-MiniLM-L12-v2` (used) | **0.85** | 0.87 | **0.81** |

The multilingual model is better overall and much better on Malay, which is where an English-only model should
struggle. A tiny check agrees: the multilingual model scores an English and a Malay café description at 0.82 and a café
versus a hotel at 0.46, while bge-small scores 0.64 and 0.55, so it separates them far less.

Caveats. There are only 169 distinct labelled descriptions (58 look Malay), so differences of a point or two mean
nothing. "Same industry" is a proxy: the industry label may be derived from the description in this synthetic data, and
two companies can be similar across industries. It does not measure ordering within the top 5. The Malay split uses a
keyword guess because the feed has no language field. I did not do the LLM explanation design.

## What is left

- The first-insert race between multiple consumers (above), and a test that runs two consumers.
- No schema migrations. Changing the schema means dropping the tables, which I did once during development. A real
  system needs Alembic or similar.
- The database-outage path in the consumer (requeue and reconnect) and RabbitMQ reconnection are written but not
  tested against a real outage.
- No replay tool for dead letters, and no metrics endpoint for the consumer.
- No authentication, rate limiting or CORS on the API. A short `GET /companies/{id}` for the page header is not built.
- Quality is only measured by industry agreement. A small hand-labelled set, or click data once it exists, would be a
  better judge.

## How I tested it

64 tests. Unit tests for parsing use real bad rows from the feed, and the merge rule is tested over every ordering of
a set of events. Integration tests run against the compose Postgres in a throwaway database with a fake embedder, and
cover the whole feed (2,000 companies, 8 struck off, every company embedded), replay, reordering, poison-message
isolation and dead letters. API tests place companies at known angles so the expected neighbours are exact, and cover
ties, limits, errors, struck-off companies and a database outage. I also ran the full path by hand: `just down`, `up`,
`publish`, `consumer`, `serve`, then a request, which returned the three companies the README mock shows first.

## AI tools used

I used Claude Code (Claude Sonnet 5.5) for most of this work: reading the task and profiling the feed, comparing
embedding models, writing the consumer, API, tests and these documents, and debugging (for example, finding that
`localhost` resolves to IPv6 first on Windows and made the database connection stall). I checked behaviour by running
it against the real feed, and the numbers above come from those runs.
