package eventlog

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/go-redis/redis/v8"
)

type EventLog struct {
	client  redis.UniversalClient
	name    string
	options *Options
}

type Options struct {
	// MaxLength is how many entries the log should keep at most. Unlimited if <= 0.
	MaxLength uint

	// TTLAfterLastAdd is how long the log will be kept around in Redis after the last entry has been added;
	// i.e. if no entries are added for this much time, the whole log will be deleted
	// Unlimited if <= 0.
	TTLAfterLastAdd time.Duration
}

type Entry map[string]interface{}

type EntryWithID struct {
	Entry Entry
	ID    string
}

// New creates a new EventLog.
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
// It returns the IDs for the given entry(-ies).
func (l *EventLog) Add(ctx context.Context, entries ...Entry) ([]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	if l.options.TTLAfterLastAdd > 0 || len(entries) > 1 {
		pipeline := l.client.Pipeline()

		for _, entry := range entries {
			l.add(ctx, pipeline, entry)
		}

		if l.options.TTLAfterLastAdd > 0 {
			pipeline.Expire(ctx, l.name, l.options.TTLAfterLastAdd)
		}

		cmders, err := pipeline.Exec(ctx)
		if err != nil {
			return nil, err
		}

		nEntries := len(entries)
		ids := make([]string, nEntries)
		for i := 0; i < nEntries; i++ {
			output := cmders[i].String()
			id := extractIDFromCmderOutput(output)
			if id == "" {
				return nil, fmt.Errorf("unable to extract entry ID from %q", output)
			}

			ids[i] = id
		}

		return ids, err
	}

	// exactly one entry, and no TTL: no need for pipelines
	id, err := l.add(ctx, l.client, entries[0]).Result()
	if err != nil {
		return nil, err
	}

	return []string{id}, nil
}

var idFromOutputRegex = regexp.MustCompile(`:\s*([\d]+-[\d]+)\s*$`)

func extractIDFromCmderOutput(output string) string {
	if match := idFromOutputRegex.FindStringSubmatch(output); len(match) == 2 { //nolint:gomnd
		return match[1]
	}
	return ""
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
func (l *EventLog) Tail(ctx context.Context) ([]EntryWithID, error) {
	return l.tailEntries(ctx, nil, nil)
}

// TailN retrieves the last n messages; n cannot be greater than MaxLength, if it's been set in the options.
// n can be 0, though it doesn't seem too interesting to do that.
func (l *EventLog) TailN(ctx context.Context, n uint) ([]EntryWithID, error) {
	return l.tailEntries(ctx, &n, nil)
}

// TailFrom retrieves the messages including and since the given entry ID.
// If it's important for the caller to ensure that they haven't missed any messages,
// they should check that the ID of the 1st returned entry is the ID they passed in.
func (l *EventLog) TailFrom(ctx context.Context, from string) ([]EntryWithID, error) {
	return l.tailEntries(ctx, nil, &from)
}

func (l *EventLog) tailEntries(ctx context.Context, n *uint, from *string) ([]EntryWithID, error) {
	messages, err := l.tailMessages(ctx, n, from)
	return messagesToEntries(messages), err
}

func (l *EventLog) tailMessages(ctx context.Context, n *uint, from *string) (messages []redis.XMessage, err error) {
	var max uint
	switch {
	case n == nil || *n > l.options.MaxLength && l.options.MaxLength != 0:
		max = l.options.MaxLength
	case *n == 0:
		return
	default:
		max = *n
	}

	start := "-"
	if from != nil {
		start = *from
	}

	if max > 0 {
		messages, err = l.client.XRevRangeN(ctx, l.name, "+", start, int64(max)).Result()
		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
			messages[i], messages[j] = messages[j], messages[i]
		}
	} else {
		messages, err = l.client.XRange(ctx, l.name, start, "+").Result()
	}

	return
}

// TailAndFollow is a blocking call. It retrieves the last MaxLength messages from the log (or all of them if the
// MaxLength option was not set), pushes them to the channel, and then proceeds to wait for new messages,
// and pushes them down the channel as they come in.
// It guarantees to push all messages exactly once.
// It will only return when it encounters an error talking to Redis, or when the context expires or gets canceled
// (in which case it returns the relevant error from the context package).
// Never returns nil.
func (l *EventLog) TailAndFollow(ctx context.Context, ch chan<- []EntryWithID) error {
	return l.tailAndFollow(ctx, nil, nil, ch)
}

// TailNAndFollow is the same as TailAndFollow, except it allows limiting how many messages are tailed initially,
// before starting to listen for new ones.
// n can be 0 here too, as for TailN, and here it can make sense: that can be used
// to subscribe to future entries regardless of past ones.
// Same as TailAndFollow, never returns nil.
func (l *EventLog) TailNAndFollow(ctx context.Context, n uint, ch chan<- []EntryWithID) error {
	return l.tailAndFollow(ctx, &n, nil, ch)
}

// TailFromAndFollow is the same as TailAndFollow, except it will limit itself to the given entry ID and the following
// entries.
// Just like TailFrom, callers should check that the first entry's ID is the one they expect if they
// want to ensure that they didn't miss anything.
// Same as TailAndFollow, never returns nil.
func (l *EventLog) TailFromAndFollow(ctx context.Context, from string, ch chan<- []EntryWithID) error {
	return l.tailAndFollow(ctx, nil, &from, ch)
}

var xReadTimeout = time.Hour // nolint: gochecknoglobals

func (l *EventLog) tailAndFollow(
	ctx context.Context, n *uint, from *string, entryChan chan<- []EntryWithID,
) (err error) {
	var messages []redis.XMessage

	nIsZero := n != nil && *n == 0
	if nIsZero {
		// still need to get the last message ID
		messages, err = l.client.XRevRangeN(ctx, l.name, "+", "-", 1).Result()
	} else {
		messages, err = l.tailMessages(ctx, n, from)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return err
	}

	var entries []EntryWithID
	if nIsZero || len(messages) == 0 {
		entries = []EntryWithID{}
	} else {
		entries = messagesToEntries(messages)
	}
	select {
	case entryChan <- entries:
	case <-ctx.Done():
		return ctx.Err()
	}

	lastMessageID := "0"
	for {
		if len(messages) != 0 {
			lastMessageID = messages[len(messages)-1].ID
		}

		result, err := l.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{l.name, lastMessageID},
			Block:   xReadTimeout,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// just means that we timed out on the "block" duration, we just come right back
				continue
			}

			return err
		}
		messages = result[0].Messages
		if len(messages) != 0 {
			select {
			case entryChan <- messagesToEntries(messages):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func messagesToEntries(messages []redis.XMessage) []EntryWithID {
	entries := make([]EntryWithID, len(messages))
	for i, message := range messages {
		entries[i] = EntryWithID{
			Entry: message.Values,
			ID:    message.ID,
		}
	}
	return entries
}
