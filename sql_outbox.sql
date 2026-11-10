CREATE TABLE IF NOT EXISTS outbox (
    id            bigserial PRIMARY KEY,
    event_type    text        NOT NULL,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    processed_at  timestamptz,
    locked_until  timestamptz,
    attempts      int         NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (id) WHERE processed_at IS NULL;

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