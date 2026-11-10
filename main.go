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
	batchSize      = 100
	maxDBParallel  = 10
	pollInterval   = 2 * time.Second
	leaseDuration  = "30 seconds"
	sqsBatchLimit  = 10 // سقف SendMessageBatch
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
			return sent, err
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
				return err 
			}
		}
		if sendErr != nil {
			return sendErr
		}
		if len(sent) < len(rows) {
			return nil 
		}
	}
}

func (r *relay) runCycle(ctx context.Context, pools map[string]*pgxpool.Pool) {
	g, gctx := errgroup.WithContext(context.Background())
	g.SetLimit(maxDBParallel)
	_ = gctx

	for name, pool := range pools {
		g.Go(func() error {
			cctx, cancel := context.WithTimeout(ctx, 25*time.Second) 
			defer cancel()
			if err := r.processDB(cctx, pool); err != nil {
				slog.Error("relay failed", "db", name, "err", err)
			}
			return nil
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

	// DSNها: DB_DSNS="postgres://u:p@h1/db1,postgres://u:p@h2/db2"
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