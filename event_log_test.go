package eventlog

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dchest/uniuri"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestThreadAndProcessSafe(t *testing.T) {
	baseCtx := withTestDeadline(t, 30*time.Second)

	client := redisClient(baseCtx, t)

	key := withTestRedisKey(baseCtx, t, client)
	log := New(client, key, nil)

	producersCount := 20
	entriesPerProducer := 20

	// each entry is pushed exactly once by one producer
	entries := randomEntries(producersCount * entriesPerProducer)

	tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
	ch := make(chan []EntryWithID)
	consumersPerProducer := 3

	var wg sync.WaitGroup
	wg.Add(producersCount * (1 + consumersPerProducer))

	entryIDsCh := make(chan []string, len(entries)+1)

	blockingCountBefore := countBlockingConnections(baseCtx, t, client)
	startTime := time.Now()

	for i := 0; i < producersCount; i++ {
		go func(producerID int) {
			defer wg.Done()

			// this way we test both sharing the same log, or using different log objects
			// sharing the same underlying redis stream
			localLog := log
			if producerID%2 == 0 {
				localLog = New(redisClient(baseCtx, t), key, nil)
			}

			for j := 0; j < entriesPerProducer; j++ {
				ids, err := localLog.Add(baseCtx, entries[producerID+j*producersCount])
				require.NoError(t, err)
				entryIDsCh <- ids
			}
		}(i)

		for k := 0; k < consumersPerProducer; k++ {
			go func(consumerID int) {
				defer wg.Done()

				// same idea as above for producers
				localLog := log
				if consumerID%2 == 0 {
					localLog = New(redisClient(baseCtx, t), key, nil)
				}

				err := localLog.TailAndFollow(tailCtx, ch)
				assert.ErrorIs(t, err, context.Canceled)
			}(i*consumersPerProducer + k)
		}
	}

	// each consumer should see each entry exactly once
	countsPerEntry := make([]int, len(entries))
	totalCount := 0
	consumersCount := producersCount * consumersPerProducer

	expectedTotalCount := consumersCount * len(entries)
	for totalCount != expectedTotalCount {
		select {
		case entrySlice := <-ch:
			for _, entry := range entrySlice {
				entryIndex, err := strconv.Atoi(entry.Entry[indexField].(string))
				if assert.NoError(t, err) {
					countsPerEntry[entryIndex]++

					expectedEntry := entries[entryIndex]
					assert.Equal(t, expectedEntry, entry.Entry)
				}
			}

			totalCount += len(entrySlice)
		case <-baseCtx.Done():
			t.Fatal("timed out waiting for entries")
		}
	}

	for entryIndex, count := range countsPerEntry {
		assert.Equal(t, consumersCount, count,
			"entry %d only appeared %d times", entryIndex, count)
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

	// last but not least, let's check the entry IDs
	entryIDs := emptyBufferedChannel(entryIDsCh, func(ids []string) {
		// we pushed one by one
		require.Equal(t, 1, len(ids))
	})
	assertEntryIDsAreValid(t, startTime, entryIDs, false)
}

func TestWithMaxLength(t *testing.T) {
	baseCtx := withTestDeadline(t, 5*time.Second)

	client := redisClient(baseCtx, t)

	key := withTestRedisKey(baseCtx, t, client)
	maxLength := 3
	log := New(client, key, &Options{
		MaxLength: uint(maxLength),
	})

	entriesCount := 200
	entries := randomEntries(entriesCount)

	startTime := time.Now()
	ids, err := log.Add(baseCtx, entries...)
	require.NoError(t, err)
	assertEntryIDsAreValid(t, startTime, ids, true)

	actual, err := log.Tail(baseCtx)
	require.NoError(t, err)
	assert.Equal(t, maxLength, len(actual))

	expectedEntries := entries[entriesCount-maxLength:]
	expectedIDs := ids[entriesCount-maxLength:]
	assert.Equal(t, zipEntriesWithIDs(t, expectedEntries, expectedIDs), actual)

	redisCount, err := client.XLen(baseCtx, key).Result()
	if assert.NoError(t, err) {
		// redis doesn't guarantee to prune every time, so we can't test equality
		assert.Less(t, redisCount, int64(entriesCount))
	}
}

func TestWithTTL(t *testing.T) {
	baseCtx := withTestDeadline(t, 30*time.Second)

	client := redisClient(baseCtx, t)

	key := withTestRedisKey(baseCtx, t, client)
	log := New(client, key, &Options{
		TTLAfterLastAdd: 2 * testInterval,
	})

	entriesCount := 4
	entries := randomEntries(entriesCount)

	tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
	ch := make(chan []EntryWithID, entriesCount+1)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err := log.TailAndFollow(tailCtx, ch)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}()

	startTime := time.Now()
	ids := make([]string, entriesCount)
	for i := 0; i < entriesCount; i++ {
		newID, err := log.Add(baseCtx, entries[i])
		require.NoError(t, err)

		require.Equal(t, 1, len(newID))
		ids[i] = newID[0]

		time.Sleep(testInterval)
	}

	assertEntryIDsAreValid(t, startTime, ids, false)

	// key still exists, even though the TTL has elapsed twice since its creation
	assert.True(t, redisKeyExists(baseCtx, t, client, key))

	time.Sleep(2 * testInterval)
	// we're now one full testInterval after it should have been deleted
	assert.False(t, redisKeyExists(baseCtx, t, client, key))

	cancelTailCtx()
	wg.Wait()

	// let's just make sure we're received all the entries, for good measure
	assert.Equal(t, zipEntriesWithIDs(t, entries, ids), emptyBufferedChannel(ch))
}

func TestTailAndFollow(t *testing.T) {
	baseCtx := withTestDeadline(t, 10*time.Second)

	client := redisClient(baseCtx, t)

	// allows testing that we use Redis' lazy pruning of streams
	seenMoreEntriesThanMax := false

	maxInitialEntries := 200
	allEntries := randomEntries(maxInitialEntries)

	for nInitialEntries := 0; nInitialEntries <= maxInitialEntries; nInitialEntries += 22 {
		for _, maxLength := range []uint{0, 25} {
			t.Run(fmt.Sprintf("with %d initial entries, and max length %d", nInitialEntries, maxLength), func(t *testing.T) {
				entries := allEntries[:nInitialEntries]

				log, ids := seedLog(baseCtx, t, client, entries, maxLength)

				actual, err := log.Tail(baseCtx)
				require.NoError(t, err)

				expectedLen := nInitialEntries
				if maxLength != 0 {
					expectedLen = min(expectedLen, maxLength)

					if !seenMoreEntriesThanMax {
						redisCount, err := client.XLen(baseCtx, log.name).Result()
						if assert.NoError(t, err) && redisCount > int64(expectedLen) {
							seenMoreEntriesThanMax = true
						}
					}
				}
				expectedEntries := entries[nInitialEntries-expectedLen:]
				expectedIDs := ids[nInitialEntries-expectedLen:]
				expected := zipEntriesWithIDs(t, expectedEntries, expectedIDs)

				require.Equal(t, expected, actual)

				testTail(baseCtx, t, log, expected, 10, log.TailAndFollow)
			})
		}
	}

	assert.True(t, seenMoreEntriesThanMax)
}

func TestTailNAndFollow(t *testing.T) {
	baseCtx := withTestDeadline(t, 10*time.Second)

	client := redisClient(baseCtx, t)

	maxInitialEntries := 50
	allEntries := randomEntries(maxInitialEntries)

	for nInitialEntries := 0; nInitialEntries <= maxInitialEntries; nInitialEntries += 22 {
		for _, maxLength := range []uint{0, 25} {
			for n := uint(0); n <= 50; n += 16 {
				testName := fmt.Sprintf("with %d initial entries, max length %d and n %d",
					nInitialEntries, maxLength, n)

				t.Run(testName, func(t *testing.T) {
					entries := allEntries[:nInitialEntries]

					log, ids := seedLog(baseCtx, t, client, entries, maxLength)

					actual, err := log.TailN(baseCtx, n)
					require.NoError(t, err)

					expectedLen := min(nInitialEntries, n)
					if maxLength != 0 {
						expectedLen = min(expectedLen, maxLength)
					}
					expectedEntries := entries[nInitialEntries-expectedLen:]
					expectedIDs := ids[nInitialEntries-expectedLen:]
					expected := zipEntriesWithIDs(t, expectedEntries, expectedIDs)

					require.Equal(t, expected, actual)

					testTail(baseCtx, t, log, expected, 10, func(ctx context.Context, ch chan<- []EntryWithID) error {
						return log.TailNAndFollow(ctx, n, ch)
					})
				})
			}
		}
	}
}

func TestTailFromAndFollow(t *testing.T) {
	baseCtx := withTestDeadline(t, 10*time.Second)

	client := redisClient(baseCtx, t)

	maxInitialEntries := 200
	allEntries := randomEntries(maxInitialEntries)

	for nInitialEntries := 50; nInitialEntries <= maxInitialEntries; nInitialEntries += 22 {
		for _, maxLength := range []uint{0, 25} {
			increment := nInitialEntries / 4

			for fromIndex := 0; fromIndex < nInitialEntries; fromIndex += increment {
				testName := fmt.Sprintf("with %d initial entries, max length %d and from index %d",
					nInitialEntries, maxLength, fromIndex)

				t.Run(testName, func(t *testing.T) {
					entries := allEntries[:nInitialEntries]

					log, ids := seedLog(baseCtx, t, client, entries, maxLength)

					entryID := ids[fromIndex]
					actual, err := log.TailFrom(baseCtx, entryID)
					require.NoError(t, err)

					expectedLen := nInitialEntries - fromIndex
					if maxLength != 0 {
						expectedLen = min(expectedLen, maxLength)
					}
					expectedEntries := entries[nInitialEntries-expectedLen:]
					expectedIDs := ids[nInitialEntries-expectedLen:]
					expected := zipEntriesWithIDs(t, expectedEntries, expectedIDs)

					require.Equal(t, expected, actual)

					if maxLength == 0 || fromIndex >= nInitialEntries-int(maxLength) {
						// first entry should be the one whose ID we passed
						assert.Equal(t, entryID, actual[0].ID)

						entryIndex, err := strconv.Atoi(actual[0].Entry[indexField].(string))
						if assert.NoError(t, err) {
							assert.Equal(t, fromIndex, entryIndex)
						}
					}

					testTail(baseCtx, t, log, expected, 10, func(ctx context.Context, ch chan<- []EntryWithID) error {
						return log.TailFromAndFollow(ctx, entryID, ch)
					})
				})
			}
		}
	}
}

// tests what happens when the Redis XREAD times out; that should be transparent to the user
func TestTailAndFollowTimeout(t *testing.T) {
	withXReadTimeout(t, testInterval)

	baseCtx := withTestDeadline(t, 30*time.Second)

	client := &redisClientWrapper{UniversalClient: redisClient(baseCtx, t)}

	key := withTestRedisKey(baseCtx, t, client)
	log := New(client, key, nil)

	nEntries := 5
	addInterval := 3 * testInterval

	testTail(baseCtx, t, log, []EntryWithID{}, nEntries, log.TailAndFollow, addInterval)

	assert.GreaterOrEqual(t, client.nXReadCalls, 2*(nEntries+1))
}

func TestTailDoesNotLockWhenTheChannelIsNotBeingReadFrom(t *testing.T) {
	baseCtx := withTestDeadline(t, 5*time.Second)

	client := redisClient(baseCtx, t)

	key := withTestRedisKey(baseCtx, t, client)
	log := New(client, key, nil)

	tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
	ch := make(chan []EntryWithID)

	var tailExited sync.WaitGroup
	tailExited.Add(1)

	go func() {
		defer tailExited.Done()

		err := log.TailAndFollow(tailCtx, ch)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	time.Sleep(testInterval)
	cancelTailCtx()
	tailExited.Wait()
}

// Helpers below

const (
	indexField = "_index"

	// shouldn't be too short to avoid edge cases/race conditions, especially in CI
	testInterval = 500 * time.Millisecond
)

func withTestDeadline(t *testing.T, duration time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	t.Cleanup(cancel)
	return ctx
}

// returns the name of a key that currently doesn't exist, and sets it up to be cleaned up when done testing
func withTestRedisKey(ctx context.Context, t *testing.T, c redis.UniversalClient) string {
	for {
		key := "go-event-log-test-" + randomString()
		if !redisKeyExists(ctx, t, c, key) {
			t.Cleanup(func() {
				require.NoError(t, c.Del(ctx, key).Err())
			})

			t.Logf("Using temp redis key %q", key)
			return key
		}
	}
}

func redisKeyExists(ctx context.Context, t *testing.T, client redis.UniversalClient, key string) bool {
	count, err := client.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.True(t, count == 0 || count == 1)
	return count == 1
}

// these tests assume that there is a redis server listening on port 6379 locally
func redisClient(ctx context.Context, t *testing.T) redis.UniversalClient {
	client := redis.NewClient(&redis.Options{})
	require.NoError(t, client.Ping(ctx).Err())

	return client
}

func randomString() string {
	return uniuri.NewLen(uniuri.UUIDLen)
}

func randomEntry(index int) Entry {
	numItems := 1 + rand.Intn(5)
	entry := make(map[string]interface{})
	for i := 0; i < numItems; i++ {
		entry[randomString()] = randomString()
	}
	entry[indexField] = strconv.Itoa(index)
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
func countBlockingConnections(ctx context.Context, t *testing.T, c redis.UniversalClient) int {
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

func emptyBufferedChannel[T any](ch chan []T, forEachAssertions ...func([]T)) (result []T) {
	for {
		select {
		case entries := <-ch:
			for _, assertion := range forEachAssertions {
				assertion(entries)
			}
			result = append(result, entries...)
		default:
			return
		}
	}
}

// checks that the entry IDs make sense: they should be of the form "timestamp-seqNumber"
// where timestamp should be between startTime and now, and seqNumbers for the same timestamp
// should be sequential from 0, and appear exactly once each.
func assertEntryIDsAreValid(t *testing.T, startTime time.Time, ids []string, expectNonZeroSeqNumbers bool) bool {
	if len(ids) == 0 {
		return true
	}

	now := time.Now()

	processed, err := parseAndProcessEntryIDs(ids)
	if !assert.NoError(t, err) {
		return false
	}

	result := assert.GreaterOrEqual(t, processed[0][0], startTime.UnixMilli(), "first event before start") &&
		assert.LessOrEqual(t, processed[len(processed)-1][0], now.UnixMilli(), "last event after now")

	if expectNonZeroSeqNumbers {
		result = assert.True(t, existsNonZeroMaxSeqNumber(processed), "expected non-zero seq numbers") && result
	}

	return result
}

// Returns a list of [timestamp, maxSeqNumber] tuples, ordered by timestamps.
func parseAndProcessEntryIDs(ids []string) ([][2]int64, error) {
	// maps each timestamp to the set of all seqNumbers we've seen for it so far
	perTimestamp := make(map[int64]map[int]bool)

	for _, id := range ids {
		timestamp, seqNumber, err := parseEntryID(id)
		if err != nil {
			return nil, err
		}

		seqNumberSet := perTimestamp[timestamp]
		switch {
		case seqNumberSet == nil:
			perTimestamp[timestamp] = map[int]bool{seqNumber: true}
		case seqNumberSet[seqNumber]:
			return nil, fmt.Errorf("duplicate seq number %d for timestamp %d", seqNumber, timestamp)
		default:
			seqNumberSet[seqNumber] = true
		}
	}

	result := make([][2]int64, 0, len(perTimestamp))
	for timestamp, seqNumberSet := range perTimestamp {
		if !isSeqNumberSetSequential(seqNumberSet) {
			return nil, fmt.Errorf("not a sequential set for timestamp %d: %v", timestamp, seqNumberSet)
		}

		result = append(result, [2]int64{timestamp, int64(len(seqNumberSet) - 1)})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i][0] < result[j][0]
	})

	return result, nil
}

var entryIDRegex = regexp.MustCompile(`^([\d]+)-([\d]+)$`)

func parseEntryID(id string) (timestamp int64, seqNumber int, err error) {
	if match := entryIDRegex.FindStringSubmatch(id); len(match) == 3 {
		timestamp, err = strconv.ParseInt(match[1], 10, 0)
		if err == nil {
			seqNumber, err = strconv.Atoi(match[2])
		}
	} else {
		err = fmt.Errorf("unable to parse entry ID %q", id)
	}

	return
}

func isSeqNumberSetSequential(seqNumberSet map[int]bool) bool {
	asList := make([]int, 0, len(seqNumberSet))
	for seqNumber := range seqNumberSet {
		asList = append(asList, seqNumber)
	}

	sort.Ints(asList)

	for expected, actual := range asList {
		if expected != actual {
			return false
		}
	}

	return true
}

func existsNonZeroMaxSeqNumber(processedEntryIDs [][2]int64) bool {
	for _, tuple := range processedEntryIDs {
		if tuple[1] != 0 {
			return true
		}
	}

	return false
}

// seedLog returns a new log seeded with the given entries
// also returns the entry IDs for the created entries.
func seedLog(
	ctx context.Context,
	t *testing.T,
	client redis.UniversalClient,
	entries []Entry,
	maxLength ...uint,
) (
	*EventLog,
	[]string,
) {
	require.LessOrEqual(t, len(maxLength), 1)
	var o *Options
	if len(maxLength) == 1 {
		o = &Options{MaxLength: maxLength[0]}
	}

	key := withTestRedisKey(ctx, t, client)

	log := New(client, key, o)

	entryIDs, err := log.Add(ctx, entries...)
	require.NoError(t, err)

	return log, entryIDs
}

// generic way of testing for Tail*AndFollow variants
func testTail(
	baseCtx context.Context,
	t *testing.T,
	log *EventLog,
	expectedInitialEntries []EntryWithID,
	newEntriesCount int,
	tailVariant func(context.Context, chan<- []EntryWithID) error,
	addInterval ...time.Duration,
) {
	require.LessOrEqual(t, len(addInterval), 1)

	tailCtx, cancelTailCtx := context.WithCancel(baseCtx)
	ch := make(chan []EntryWithID)

	var tailExited sync.WaitGroup
	tailExited.Add(1)

	go func() {
		defer tailExited.Done()

		err := tailVariant(tailCtx, ch)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	select {
	case initialEntries := <-ch:
		assert.Equal(t, expectedInitialEntries, initialEntries, "initial entries")
	case <-baseCtx.Done():
		t.Fatalf("timed out waiting for initial entries")
	}

	newEntries := randomEntries(newEntriesCount)
	newIDsCh := make(chan string, newEntriesCount+1)

	go func() {
		for _, entry := range newEntries {
			newID, err := log.Add(baseCtx, entry)
			require.NoError(t, err)

			require.Equal(t, 1, len(newID))
			newIDsCh <- newID[0]

			if len(addInterval) == 1 {
				time.Sleep(addInterval[0])
			}
		}
	}()

	actual := make([]EntryWithID, 0, newEntriesCount)
	newIDs := make([]string, 0, newEntriesCount)
	for len(actual) < newEntriesCount || len(newIDs) < newEntriesCount {
		select {
		case entries := <-ch:
			actual = append(actual, entries...)
		case id := <-newIDsCh:
			newIDs = append(newIDs, id)
		case <-baseCtx.Done():
			t.Fatalf("timed out waiting for entries & IDs; current lengths: %d and %d", len(actual), len(newIDs))
		}
	}

	cancelTailCtx()
	tailExited.Wait()

	expectedNewEntries := zipEntriesWithIDs(t, newEntries, newIDs)
	assert.Equal(t, expectedNewEntries, actual, "new entries")
}

func min(items ...any) int {
	if len(items) == 0 {
		panic("need at least 1 arg")
	}

	result := math.MaxInt
	for _, x := range items {
		i := toInt(x)
		if i < result {
			result = i
		}
	}
	return result
}

func toInt(x any) int {
	switch i := x.(type) {
	case int:
		return i
	case uint:
		return int(i)
	default:
		panic(fmt.Sprintf("don't know how to cast %#v to int", x))
	}
}

func withXReadTimeout(t *testing.T, duration time.Duration) {
	previous := xReadTimeout
	xReadTimeout = duration
	t.Cleanup(func() {
		xReadTimeout = previous
	})
}

// allows counting the XREAD calls, and inspecting how many messages were retrieved
type redisClientWrapper struct {
	redis.UniversalClient

	nXReadCalls        int
	xReadMessageCounts []int
}

func (w *redisClientWrapper) XRead(ctx context.Context, args *redis.XReadArgs) *redis.XStreamSliceCmd {
	w.nXReadCalls++

	return w.UniversalClient.XRead(ctx, args)
}

func zipEntriesWithIDs(t *testing.T, entries []Entry, ids []string) []EntryWithID {
	require.Equal(t, len(entries), len(ids), "different lengths for entries and IDs")

	withIDs := make([]EntryWithID, len(entries))
	for i, entry := range entries {
		withIDs[i] = EntryWithID{
			Entry: entry,
			ID:    ids[i],
		}
	}

	return withIDs
}
