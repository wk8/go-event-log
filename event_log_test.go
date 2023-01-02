package eventlog

import (
	"context"
	"github.com/dchest/uniuri"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TODO wkpo:
// * basic
// * multi consumers/providers
// TODO wkpo toujours relevant ca ^ ?

func TestEventLog(t *testing.T) {
	rand.Seed(time.Now().Unix())

	baseCtx, cancelBaseCtx := context.WithTimeout(context.Background(), time.Minute)
	defer cancelBaseCtx()

	client := redisClient(baseCtx, t)

	t.Run("basic functionality", func(t *testing.T) {
		key := withTestRedisKey(baseCtx, t, client)
		log := New(client, key, nil)

		producersCount := 20
		entriesPerProducer := 20

		// each entry is pushed exactly once by one producer
		entries := randomEntries(producersCount * entriesPerProducer)

		tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
		ch := make(chan []Entry)
		consumersPerProducer := 3

		var wg sync.WaitGroup
		wg.Add(producersCount * (1 + consumersPerProducer))

		blockingCountBefore := countBlockingConnections(baseCtx, t, client)

		for i := 0; i < producersCount; i++ {
			go func(producerId int) {
				defer wg.Done()

				for j := 0; j < entriesPerProducer; j++ {
					err := log.Add(baseCtx, entries[producerId+j*producersCount])
					require.NoError(t, err)
				}
			}(i)

			for k := 0; k < consumersPerProducer; k++ {
				go func() {
					defer wg.Done()

					err := log.TailAndFollow(tailCtx, ch)
					assert.ErrorIs(t, err, context.Canceled)
				}()
			}
		}

		// each consumer should see each entry exactly once
		countsPerEntry := make([]int, len(entries))
		totalCount := 0
		consumersCount := producersCount * consumersPerProducer

		for totalCount != consumersCount*len(entries) {
			select {
			case entrySlice := <-ch:
				for _, entry := range entrySlice {
					entryID, err := strconv.Atoi(entry[idField].(string))
					if assert.NoError(t, err) {
						countsPerEntry[entryID]++

						expectedEntry := entries[entryID]
						assert.Equal(t, expectedEntry, entry)
					}
				}

				totalCount += len(entrySlice)
			case <-baseCtx.Done():
				t.Fatal("timed out waiting for entries")
			}
		}

		for entryID, count := range countsPerEntry {
			assert.Equal(t, consumersCount, count,
				"entry %d only appeared %d times", entryID, count)
		}

		time.Sleep(testInterval)
		blockingCountDuring := countBlockingConnections(baseCtx, t, client)
		cancelTailCtx()
		wg.Wait()
		time.Sleep(testInterval)
		blockingCountAfter := countBlockingConnections(baseCtx, t, client)

		// these assertions can fail if the redis we're using actually runs other stuff in parallel
		// still good to have
		assert.Equal(t, blockingCountDuring-blockingCountBefore, consumersCount)
		assert.Equal(t, blockingCountAfter, blockingCountBefore)
	})

	t.Run("with a max length", func(t *testing.T) {
		key := withTestRedisKey(baseCtx, t, client)
		maxLength := 3
		log := New(client, key, &Options{
			MaxLength: maxLength,
		})

		entriesCount := 200
		entries := randomEntries(entriesCount)

		require.NoError(t, log.Add(baseCtx, entries...))

		e, err := log.Tail(baseCtx)
		require.NoError(t, err)
		assert.Equal(t, maxLength, len(e))
		assert.Equal(t, entries[entriesCount-maxLength:], e)

		redisCount, err := client.XLen(baseCtx, key).Result()
		if assert.NoError(t, err) {
			// redis doesn't guarantee to prune every time, so we can't test equality
			assert.Less(t, redisCount, int64(entriesCount))
		}
	})

	t.Run("with a TTL", func(t *testing.T) {
		key := withTestRedisKey(baseCtx, t, client)
		log := New(client, key, &Options{
			TTLAfterLastAdd: 2 * testInterval,
		})

		entriesCount := 4
		entries := randomEntries(entriesCount)

		tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
		ch := make(chan []Entry, entriesCount+1)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			err := log.TailAndFollow(tailCtx, ch)
			assert.ErrorIs(t, err, context.Canceled)
			wg.Done()
		}()

		for i := 0; i < entriesCount; i++ {
			require.NoError(t, log.Add(baseCtx, entries[i]))
			time.Sleep(testInterval)
		}

		// key still exists, even though the TTL has elapsed twice since its creation
		assert.True(t, redisKeyExists(baseCtx, t, client, key))

		time.Sleep(2 * testInterval)
		// we're now one full testInterval after it should have been deleted
		assert.False(t, redisKeyExists(baseCtx, t, client, key))

		cancelTailCtx()
		wg.Wait()

		// let's just make sure we're received all the entries
		assert.Equal(t, entries, emptyBufferedChannel(ch))
	})
}

// Helpers below

const (
	idField = "_id"
	// shouldn't be too short to avoid edge cases/race conditions, especially in CI
	testInterval = 500 * time.Millisecond
)

// returns the name of a key that currently doesn't exist, and sets it up to be cleaned up when done testing
func withTestRedisKey(ctx context.Context, t *testing.T, c *redis.Client) string {
	for {
		key := "go-event-log-test-" + randomString()
		if !redisKeyExists(ctx, t, c, key) {
			t.Cleanup(func() {
				require.NoError(t, c.Del(ctx, key).Err())
			})

			return key
		}
	}
}

func redisKeyExists(ctx context.Context, t *testing.T, c *redis.Client, key string) bool {
	count, err := c.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.True(t, count == 0 || count == 1)
	return count == 1
}

// these tests assume that there is a redis server listening on port 6379 locally
func redisClient(ctx context.Context, t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{})
	require.NoError(t, client.Ping(ctx).Err())

	return client
}

func randomString() string {
	return uniuri.NewLen(uniuri.UUIDLen)
}

func randomEntry(id int) Entry {
	numItems := 1 + rand.Intn(5)
	entry := make(map[string]interface{})
	for i := 0; i < numItems; i++ {
		entry[randomString()] = randomString()
	}
	entry[idField] = strconv.Itoa(id)
	return entry
}

func randomEntries(n int) []Entry {
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = randomEntry(i)
	}
	return entries
}

// counts Redis connections that are in the "blocking" state, i.e. that are waiting for
// an event
func countBlockingConnections(ctx context.Context, t *testing.T, c *redis.Client) int {
	clientList, err := c.ClientList(ctx).Result()
	require.NoError(t, err)

	count := 0
	for _, client := range parseClientListOutput(t, clientList) {
		if strings.Contains(client["flags"], "b") {
			count++
		}
	}
	return count
}

// see https://redis.io/commands/client-list/
func parseClientListOutput(t *testing.T, rawOutput string) []map[string]string {
	lines := strings.Split(rawOutput, "\n")
	if len(lines) != 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	result := make([]map[string]string, len(lines))

	for i, line := range lines {
		require.NotEqual(t, 0, len(line))
		client := make(map[string]string)

		for _, keyValue := range strings.Split(line, " ") {
			splitKeyValue := strings.Split(keyValue, "=")

			switch len(splitKeyValue) {
			case 2:
				client[splitKeyValue[0]] = splitKeyValue[1]
			case 1:
				require.Equal(t, "=", keyValue[len(keyValue)-1])
				client[splitKeyValue[0]] = ""
			default:
				t.Fatalf("unexpected key/value pair in client list: %q", keyValue)
			}
		}

		result[i] = client
	}

	return result
}

func emptyBufferedChannel(ch chan []Entry) (result []Entry) {
	for {
		select {
		case entries := <-ch:
			result = append(result, entries...)
		default:
			return
		}
	}
}
