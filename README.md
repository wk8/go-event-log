[![Build Status](https://circleci.com/gh/wk8/go-event-log.svg?style=svg)](https://app.circleci.com/pipelines/github/wk8/go-event-log)

# go-event-logs

Redis-backed distributed event logs for golang.

## Why?

Sometimes you need a lightweight way to centralize an even log for events related to a same entity; so that multiple writers can push events to the log, and multiple readers can read those events, and get notifications when new events are added.

If you already have a redis in use in your infrastructure, this library allows you to leverage [redis streams](https://redis.io/docs/data-types/streams-tutorial/) to easily maintain and access distributed event logs.

## Usage

```go
package main

import (
	"context"

	"github.com/redis/go-redis/v9"
	eventlog "github.com/wk8/go-event-log"
)

func example(ctx context.Context, redisClient redis.UniversalClient) error {
	log := eventlog.New(redisClient, "redisKey", nil)

	// OR if you want your log to have a TTL, and/or a max length
	log := eventlog.New(redisClient, "redisKey", &eventlog.Options{
		// MaxLength is how many entries the log should keep at most. Unlimited if <= 0.
		MaxLength: 1000,

		// TTLAfterLastAdd is how long the log will be kept around in Redis after the last entry has been added;
		// i.e. if no entries are added for this much time, the whole log will be deleted
		// Unlimited if <= 0.
		TTLAfterLastAdd: time.Hour,
	})

	// to add events to it:
	eventIDs, err := log.Add(ctx,
		map[string]any{
			"sensor_id":   1234,
			"temperature": 19.8,
		},
		map[string]any{
			"sensor_id":   1234,
			"temperature": 19.8,
		},
	)
	if err != nil {
		return err
	}
	// eventIDs's length is equal to the number of events passed to `Add`.
	// All event IDs have the form "<timestamp>_<sequenceNumber>"

	// reading from a log:
	// 1. the whole log:
	eventWithIDs, err := log.Tail(ctx)

	// 2. or just the last N events:
	eventWithIDs, err := log.TailN(ctx, 100)

	// 3. or just the new events since a specific event:
	eventWithIDs, err := log.TailFrom(ctx, eventIDs[1])

	// All three read variants come with `...AndFollow` variants, which allow
	// subscribing to new events coming in after reading events that already exist, for example:
	ch := make(chan []eventlog.EntryWithID)
	go func() {
		// this is a blocking call, and will only return when it encounters an error
		// one can simply cancel the context to make it return
		if err := log.TailFromAndFollow(ctx, eventIDs[1], ch); err != nil {
			// ...
		}
	}()

	// wait for new events:
	for {
		eventsWithIDs := <-ch

		// do something with them
	}
}
```

You can also [refer to the tests for some more advanced example use cases](https://github.com/wk8/go-event-log/blob/main/event_log_test.go).

## Guarantees

All events added to the log will be read in the order they were added; and all the `...AndFollow` methods guarantee that the callers will see exactly each new event once.
