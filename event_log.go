package eventlog

// TODO wkpo

// TODO wkpo re-order?
// TODO wkpo mod tidy? prolly don't need errors wrapper any more?
import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type EventLog struct {
	client  redis.UniversalClient
	name    string
	options *Options
}

type Options struct {
	// MaxLength is how many entries the log should keep at most. Unlimited if <= 0.
	MaxLength int

	// TODO wkpo tests on this!!
	// TTLAfterLastAdd is how long the log will be kept around in Redis after the last entry has been added;
	// i.e. if no entries are added for this much time, the whole log will be deleted
	// Unlimited if <= 0.
	TTLAfterLastAdd time.Duration
}

type Entry map[string]interface{}

func New(client redis.UniversalClient, name string, options *Options) *EventLog {
	if options == nil {
		options = &Options{}
	}

	return &EventLog{
		client:  client,
		name:    name,
		options: options,
	}
}

// Add adds one or several entries to the log.
func (l *EventLog) Add(ctx context.Context, entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}

	if l.options.TTLAfterLastAdd > 0 || len(entries) > 1 {
		pipeline := l.client.Pipeline()

		for _, entry := range entries {
			l.add(ctx, pipeline, entry)
		}

		if l.options.TTLAfterLastAdd > 0 {
			pipeline.Expire(ctx, l.name, l.options.TTLAfterLastAdd)
		}

		_, err := pipeline.Exec(ctx)
		return err
	}

	// exactly one entry, and no TTL: no need for pipelines
	return l.add(ctx, l.client, entries[0]).Err()
}

func (l *EventLog) add(ctx context.Context, client redis.Cmdable, entry Entry) *redis.StringCmd {
	addArgs := &redis.XAddArgs{
		Stream: l.name,
		Values: map[string]interface{}(entry),
	}

	if l.options.MaxLength > 0 {
		addArgs.MaxLen = int64(l.options.MaxLength)
		addArgs.Approx = true
	}

	return client.XAdd(ctx, addArgs)
}

// Tail retrieves the last messages committed to the event log - either the whole log, if the MaxLength option
// hasn't been set, or just the last MaxLength messages.
func (l *EventLog) Tail(ctx context.Context) ([]Entry, error) {
	return l.TailN(ctx, l.options.MaxLength)
}

// TailN retrieves the last n messages; n cannot be greater than MaxLength, if it's been set in the options.
func (l *EventLog) TailN(ctx context.Context, n int) ([]Entry, error) {
	messages, err := l.tailN(ctx, n)
	if err != nil {
		return nil, err
	}

	return messagesToEntries(messages), nil
}

func (l *EventLog) tailN(ctx context.Context, n int) ([]redis.XMessage, error) {
	if n > l.options.MaxLength && l.options.MaxLength > 0 {
		n = l.options.MaxLength
	}

	// TODO wkpo au lieu de +/- on pourrait donner des ID de stop/end aussi? pourrait etre utile pour our use case?
	if n > 0 {
		messages, err := l.client.XRevRangeN(ctx, l.name, "+", "-", int64(n)).Result()
		if err != nil {
			return nil, err
		}

		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
			messages[i], messages[j] = messages[j], messages[i]
		}

		return messages, nil
	} else {
		return l.client.XRange(ctx, l.name, "-", "+").Result()
	}
}

// TailAndFollow is a blocking call. It retrieves the last MaxLength messages from the log (or all of them if the
// MaxLength option was not set), pushes them to the channel, and then proceeds to wait for new messages.
// and pushes them down the channel as they come in.
// It guarantees to push all messages exactly once.
// It will only return when it encounters an error talking to Redis, or when the context expires or gets canceled
// (in which case it returns the relevant error from the context package).
// Never returns nil.
func (l *EventLog) TailAndFollow(ctx context.Context, ch chan<- []Entry) error {
	return l.TailNAndFollow(ctx, l.options.MaxLength, ch)
}

// TailNAndFollow is the same as TailAndFollow, except it allows limiting how many messages are tailed initially,
// before starting to listen for new ones.
func (l *EventLog) TailNAndFollow(ctx context.Context, n int, ch chan<- []Entry) error {
	messages, err := l.tailN(ctx, n)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return err
	}
	ch <- messagesToEntries(messages)

	lastMessageID := "0"
	for {
		if len(messages) != 0 {
			lastMessageID = messages[len(messages)-1].ID
		}

		result, err := l.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{l.name, lastMessageID},
			Block:   time.Hour,
		}).Result()

		if err != nil {
			return err
		}
		messages = result[0].Messages
		ch <- messagesToEntries(messages)

		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func messagesToEntries(messages []redis.XMessage) []Entry {
	entries := make([]Entry, len(messages))
	for i, message := range messages {
		entries[i] = message.Values
	}
	return entries
}
