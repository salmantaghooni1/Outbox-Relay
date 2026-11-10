# Outbox Relay: Running One Query Concurrently Across Many Databases and Publishing to SQS

This document describes an implementation of the **Transactional Outbox pattern** where a relay process polls the `outbox` table in **many databases concurrently** and publishes the pending events to **Amazon SQS**.

Two implementations are provided:

1. **Python** (asyncio + asyncpg + boto3), which uses a transaction with `FOR UPDATE SKIP LOCKED`.
2. **SQL functions + Go** (pgx + AWS SDK v2), which uses a **lease** (time-limited lock) so no database transaction stays open while calling SQS.

> Note: the code was not compiled or executed in the authoring environment (package installs were unavailable). Run `go build` / a smoke test before using it.

---

## 1. Concepts

### Outbox pattern
A service writes its business data and an event row into the `outbox` table in the **same local transaction**. A separate relay reads unprocessed rows and publishes them to a broker. This avoids the dual-write problem (DB commit succeeds but the publish fails, or vice versa).

### Delivery guarantee
The relay publishes to SQS first and marks the row as processed afterwards. If the process crashes between the two steps, the message is sent again. This gives **at-least-once** delivery, not exactly-once. Consumers must be **idempotent**, for example by tracking the `outbox_id` message attribute in a `processed_messages` table.

### Concurrency model
- One task/goroutine per database, with a **cap** on how many run at the same time (semaphore / `errgroup.SetLimit`).
- Several relay instances can run in parallel for horizontal scaling. `FOR UPDATE SKIP LOCKED` ensures they never pick the same rows.
- A failure in one database is logged and does not stop the others.

---

## 2. Table schema and index

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id            bigserial PRIMARY KEY,
    event_type    text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    processed_at  timestamptz,
    locked_until  timestamptz,           -- used by the lease-based version
    attempts      int         NOT NULL DEFAULT 0
);

-- Partial index: only pending rows, keeps the claim query fast
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (id) WHERE processed_at IS NULL;
```

For the Python version only `id`, `event_type`, `payload`, `processed_at` are required.

---

## 3. Version A: Python (transaction-based)

```python
import asyncio
import json
import logging

import asyncpg
import boto3

log = logging.getLogger("outbox-relay")

QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/my-queue"
BATCH_SIZE = 100            # rows read per database per round
MAX_DB_CONCURRENCY = 10     # max databases processed at the same time
POLL_INTERVAL = 2           # seconds

sqs = boto3.client("sqs")


def send_to_sqs(rows) -> set[int]:
    """Send in batches (max 10 messages per request). Returns IDs of successful rows."""
    succeeded: set[int] = set()

    for i in range(0, len(rows), 10):
        chunk = rows[i:i + 10]
        entries = [
            {
                "Id": str(r["id"]),
                "MessageBody": r["payload"],  # JSON string
                "MessageAttributes": {
                    "event_type": {"DataType": "String", "StringValue": r["event_type"]},
                    "outbox_id": {"DataType": "Number", "StringValue": str(r["id"])},
                },
            }
            for r in chunk
        ]
        resp = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)

        for ok in resp.get("Successful", []):
            succeeded.add(int(ok["Id"]))
        for fail in resp.get("Failed", []):
            log.warning("SQS failed id=%s code=%s", fail["Id"], fail["Code"])

    return succeeded


async def process_database(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        while True:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, event_type, payload::text AS payload
                    FROM outbox
                    WHERE processed_at IS NULL
                    ORDER BY id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                    BATCH_SIZE,
                )
                if not rows:
                    return

                # boto3 is blocking, so run it in a worker thread
                sent_ids = await asyncio.to_thread(send_to_sqs, rows)

                if sent_ids:
                    await conn.execute(
                        "UPDATE outbox SET processed_at = now() WHERE id = ANY($1::bigint[])",
                        list(sent_ids),
                    )

                if len(sent_ids) < len(rows):
                    return  # some messages failed; they will be retried next cycle
    finally:
        await conn.close()


async def run_cycle(dsns: list[str]) -> None:
    sem = asyncio.Semaphore(MAX_DB_CONCURRENCY)

    async def guarded(dsn: str):
        async with sem:
            try:
                await process_database(dsn)
            except Exception:
                log.exception("relay failed for %s", dsn.split("@")[-1])  # don't leak the password

    await asyncio.gather(*(guarded(d) for d in dsns))


async def main(dsns: list[str]) -> None:
    while True:
        await run_cycle(dsns)
        await asyncio.sleep(POLL_INTERVAL)
```

### Explanation

1. **Concurrency across databases.** `asyncio.gather` creates one task per database. The `Semaphore` limits simultaneous work so 200 databases do not mean 200 open connections and 200 parallel SQS calls. Exceptions are caught inside `guarded`, so one failing database does not cancel the rest.
2. **`FOR UPDATE SKIP LOCKED`.** When several relay instances run, each one only receives rows that no other instance has locked. No message is claimed by two relays, and nobody waits on anybody.
3. **SQS sending.** `send_message_batch` accepts at most 10 entries, so rows are chunked. The response can be partial (`Successful` and `Failed`); only successful rows get `processed_at`, the rest stay pending. Because `boto3` is synchronous, it runs via `asyncio.to_thread`. `aioboto3` is a fully async alternative.
4. **Trade-off.** The transaction stays open while SQS is being called. With a small `BATCH_SIZE` this is fine, but if SQS becomes slow, row locks are held longer. Version B avoids this.

---

## 4. Version B: SQL functions + Go (lease-based)

### 4.1 SQL functions

```sql
-- 1) Claim a batch and put a time-limited lock (lease) on it
CREATE OR REPLACE FUNCTION outbox_claim_batch(
    p_limit int,
    p_lease interval DEFAULT interval '30 seconds'
)
RETURNS TABLE (id bigint, event_type text, payload text)
LANGUAGE sql AS $$
    UPDATE outbox o
       SET locked_until = now() + p_lease,
           attempts     = o.attempts + 1
     WHERE o.id IN (
            SELECT b.id
              FROM outbox b
             WHERE b.processed_at IS NULL
               AND (b.locked_until IS NULL OR b.locked_until < now())
             ORDER BY b.id
             LIMIT p_limit
               FOR UPDATE SKIP LOCKED
     )
    RETURNING o.id, o.event_type, o.payload::text;
$$;

-- 2) Mark successfully published rows as processed
CREATE OR REPLACE FUNCTION outbox_mark_processed(p_ids bigint[])
RETURNS int
LANGUAGE sql AS $$
    WITH u AS (
        UPDATE outbox
           SET processed_at = now(),
               locked_until = NULL
         WHERE id = ANY(p_ids)
           AND processed_at IS NULL
        RETURNING 1
    )
    SELECT count(*)::int FROM u;
$$;
```

### 4.2 Go relay

Requires **Go 1.22+** (uses the builtin `min` and per-iteration loop variables).

```bash
go get github.com/jackc/pgx/v5 github.com/aws/aws-sdk-go-v2/config \
       github.com/aws/aws-sdk-go-v2/service/sqs golang.org/x/sync/errgroup
```

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize     = 100
	maxDBParallel = 10
	pollInterval  = 2 * time.Second
	leaseDuration = "30 seconds"
	sqsBatchLimit = 10 // SendMessageBatch hard limit
)

type outboxRow struct {
	ID        int64
	EventType string
	Payload   string
}

type relay struct {
	sqs      *sqs.Client
	queueURL string
}

// claim takes pending rows and puts a lease on them
func claim(ctx context.Context, pool *pgxpool.Pool) ([]outboxRow, error) {
	rs, err := pool.Query(ctx,
		`SELECT id, event_type, payload FROM outbox_claim_batch($1, $2::interval)`,
		batchSize, leaseDuration)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var rows []outboxRow
	for rs.Next() {
		var r outboxRow
		if err := rs.Scan(&r.ID, &r.EventType, &r.Payload); err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return rows, rs.Err()
}

// send publishes rows to SQS in batches and returns the IDs that succeeded
func (r *relay) send(ctx context.Context, rows []outboxRow) ([]int64, error) {
	var sent []int64

	for i := 0; i < len(rows); i += sqsBatchLimit {
		end := min(i+sqsBatchLimit, len(rows))
		entries := make([]types.SendMessageBatchRequestEntry, 0, end-i)

		for _, row := range rows[i:end] {
			id := strconv.FormatInt(row.ID, 10)
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id:          aws.String(id),
				MessageBody: aws.String(row.Payload),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"event_type": {DataType: aws.String("String"), StringValue: aws.String(row.EventType)},
					"outbox_id":  {DataType: aws.String("Number"), StringValue: aws.String(id)},
				},
			})
		}

		out, err := r.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(r.queueURL),
			Entries:  entries,
		})
		if err != nil {
			return sent, err // whole request failed; earlier successes are still returned
		}

		for _, ok := range out.Successful {
			id, _ := strconv.ParseInt(aws.ToString(ok.Id), 10, 64)
			sent = append(sent, id)
		}
		for _, f := range out.Failed {
			slog.Warn("sqs entry failed", "id", aws.ToString(f.Id), "code", aws.ToString(f.Code))
		}
	}
	return sent, nil
}

// processDB keeps going until the outbox of one database is drained
func (r *relay) processDB(ctx context.Context, pool *pgxpool.Pool) error {
	for {
		rows, err := claim(ctx, pool)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		sent, sendErr := r.send(ctx, rows)

		if len(sent) > 0 {
			if _, err := pool.Exec(ctx, `SELECT outbox_mark_processed($1)`, sent); err != nil {
				return err // rows are re-sent after the lease expires (at-least-once)
			}
		}
		if sendErr != nil {
			return sendErr
		}
		if len(sent) < len(rows) {
			return nil // failed rows are re-claimed after the lease expires
		}
	}
}

func (r *relay) runCycle(ctx context.Context, pools map[string]*pgxpool.Pool) {
	var g errgroup.Group
	g.SetLimit(maxDBParallel)

	for name, pool := range pools {
		g.Go(func() error {
			cctx, cancel := context.WithTimeout(ctx, 25*time.Second) // shorter than the lease
			defer cancel()
			if err := r.processDB(cctx, pool); err != nil {
				slog.Error("relay failed", "db", name, "err", err)
			}
			return nil // an error in one database must not stop the others
		})
	}
	_ = g.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	r := &relay{sqs: sqs.NewFromConfig(cfg), queueURL: os.Getenv("QUEUE_URL")}

	// DB_DSNS="postgres://u:p@h1/db1,postgres://u:p@h2/db2"
	pools := map[string]*pgxpool.Pool{}
	for i, dsn := range strings.Split(os.Getenv("DB_DSNS"), ",") {
		pc, err := pgxpool.ParseConfig(strings.TrimSpace(dsn))
		if err != nil {
			panic(err)
		}
		pc.MaxConns = 2
		pool, err := pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			panic(err)
		}
		defer pool.Close()
		pools["db"+strconv.Itoa(i)] = pool
	}

	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		r.runCycle(ctx, pools)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
```

### Explanation

1. **Why a lease instead of an open transaction?** In Version A the transaction stays open for the whole SQS call. Here `outbox_claim_batch` only updates `locked_until` and commits immediately. If SQS is slow or the relay crashes, no database lock is held; after 30 seconds the rows become claimable again.
2. **`FOR UPDATE SKIP LOCKED` inside the function.** Multiple relays can call the function at the same time and receive different rows. The `locked_until < now()` condition also prevents claiming rows that still have an active lease.
3. **Concurrency across databases.** `errgroup` with `SetLimit(maxDBParallel)` runs one goroutine per database. Each database has its own connection pool. An error in one database is logged only.
4. **Delivery guarantee.** "Publish to SQS, then `outbox_mark_processed`" is at-least-once. A crash between the two steps leads to a re-send after the lease expires. Consumers must deduplicate by `outbox_id`.
5. **Timeout versus lease.** The per-cycle timeout (25 s) is intentionally shorter than the lease (30 s), so the relay stops working before the lock is released, which reduces duplicate sends.

---

## 5. Operational notes

- **Ordering.** For ordered delivery use an SQS **FIFO queue** and add `MessageGroupId` (for example `aggregate_id`) and `MessageDeduplicationId` (the `outbox_id`) to each entry. In the lease-based version the claim query must also preserve order within each group, which makes it more complex.
- **Poison messages.** The `attempts` column lets you detect rows that keep failing. After N attempts, move them to a dead-letter table and alert.
- **Cleanup.** Periodically delete or archive rows where `processed_at` is older than your retention window, otherwise the table grows without bound.
- **Dynamic database list.** The list of DSNs can be loaded from a central tenant registry and refreshed every few minutes; create or close pools as databases appear or disappear.
- **Secrets.** Never log full DSNs. The examples only log the host part (Python) or a logical name (Go).
- **Choosing a version.** Use Version A for simplicity and small batches. Use Version B when SQS latency is variable or you want to avoid long-held row locks.