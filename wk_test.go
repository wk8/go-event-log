package eventlog

import (
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEmptyKeys(t *testing.T) {
	baseCtx := withTestDeadline(t, 30*time.Second)
	c := redisClient(baseCtx, t)

	iDontExist := "i sure as hell don't exist, trust me"

	emptyHash, err := c.HGetAll(baseCtx, iDontExist).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, emptyHash)

	emptyVal, err := c.Get(baseCtx, iDontExist).Result()
	assert.True(t, errors.Is(err, redis.Nil))
	assert.Equal(t, "", emptyVal)
}
