package timewindow

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Window struct {
	timeWindow time.Duration
	limitTimes int64
	blockTime  time.Duration
	prefix     string // redis key prefix

	rds redis.UniversalClient
}

func WithPrefix(s string) func(w *Window) {
	return func(w *Window) {
		w.prefix = s
	}
}

func NewWindow(client redis.UniversalClient, limitTimes int64, timeWindow, blockTime time.Duration, opts ...func(w *Window)) *Window {
	w := &Window{
		timeWindow: timeWindow,
		limitTimes: limitTimes,
		blockTime:  blockTime,
		prefix:     "window",
		rds:        client,
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

const addBlockLuaScript = `
local key = KEYS[1]  
local blockKey = KEYS[2]
local blockTime = ARGV[4]
local time = ARGV[3]  
local windowTime = ARGV[2]  
local count = ARGV[1]

local blocked = redis.call('exists', blockKey)
if blocked == 1 then
	return 1
end

local len = redis.call('llen', key)
if tonumber(len) < tonumber(count) then  
   	redis.call('lpush', key, time)
	redis.call('expire', key, windowTime)
	return 0  
end

local earlyTime = redis.call('lindex', key, tonumber(len) - 1)  
if tonumber(time) - tonumber(earlyTime) < tonumber(windowTime) then  
	redis.call('del', key)
	redis.call('setex', blockKey, blockTime, 1)
	return 1
end

redis.call('expire', key, windowTime)
redis.call('rpop', key)
redis.call('lpush', key, time)
return 0
`

const addBlockTimeLuaScript = `
local key = KEYS[1]
local time = ARGV[1]
local ttl = redis.call('ttl', key)
if ttl > 0 then
	redis.call('expire', key, ttl + time)
end
return 0
`

var addBlockScript = redis.NewScript(addBlockLuaScript)
var addBlockTimeScript = redis.NewScript(addBlockTimeLuaScript)

// AddBlock calculate user block status. Adaptive high concurrency.
func (w *Window) AddBlock(ctx context.Context, key string) (bool, error) {
	key = fmt.Sprintf("%s:%s", w.prefix, key)
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)

	r, err := addBlockScript.Run(ctx, w.rds, []string{key, blockKey},
		w.limitTimes,
		w.timeWindow.Seconds(),
		time.Now().Unix(),
		w.blockTime.Seconds(),
	).Int()
	return r == 1, err
}

// IsBlocked return blocked or no, ignore error
func (w *Window) IsBlocked(ctx context.Context, key string) bool {
	r, _ := w.GetBlockStatus(ctx, key)
	return r
}

// GetBlockStatus get user block status
func (w *Window) GetBlockStatus(ctx context.Context, key string) (bool, error) {
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)
	r, err := w.rds.Exists(ctx, blockKey).Result()
	return r == 1, err
}

// ClearBlockStatus delete block key
func (w *Window) ClearBlockStatus(ctx context.Context, key string) error {
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)
	return w.rds.Del(ctx, blockKey).Err()
}

// SetBlock create block key and set expire duration
func (w *Window) SetBlock(ctx context.Context, key string, duration time.Duration) error {
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)
	return w.rds.SetEx(ctx, blockKey, 1, duration).Err()
}

// AddBlockTime block key ttl + duration
func (w *Window) AddBlockTime(ctx context.Context, key string, duration time.Duration) error {
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)

	return addBlockTimeScript.Run(ctx, w.rds, []string{blockKey}, duration.Seconds()).Err()
}

// GetBlockTTL gets the remaining lock time
func (w *Window) GetBlockTTL(ctx context.Context, key string) (time.Duration, error) {
	blockKey := fmt.Sprintf("%s:block:%s", w.prefix, key)
	return w.rds.TTL(ctx, blockKey).Result()
}
