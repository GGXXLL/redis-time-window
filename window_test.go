package timewindow

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	if os.Getenv("REDIS_ADDR") == "" {
		fmt.Println("set REDIS_ADDR to run tests")
		os.Exit(0)
	}
	m.Run()
}

func TestWindow_NeedBlock(t *testing.T) {
	key := "foo"

	cli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: strings.Split(os.Getenv("REDIS_ADDR"), ","),
	})
	win := Window{
		timeWindow: 2 * time.Second,
		limitTimes: 1,
		prefix:     "test",
		rds:        cli,
		blockTime:  2 * time.Second,
	}
	blockRecords := []bool{
		false, true, true, false, true,
	}
	for i := 0; i < len(blockRecords); i++ {
		time.Sleep(time.Second)
		blocked, err := win.AddBlock(context.TODO(), key)
		if err != nil {
			t.Fatal(err)
		}
		if blocked != blockRecords[i] {
			t.Fatal("should be equal")
		}
	}

	err := win.ClearBlockStatus(context.TODO(), key)
	if err != nil {
		t.Fatal(err)
	}

	blocked, err := win.GetBlockStatus(context.TODO(), key)
	if err != nil {
		t.Fatal(err)
	}
	if blocked {
		t.Fatal("should be not blocked")
	}
}

func TestWindow_ChangeBlockTime(t *testing.T) {
	key := "bar"

	cli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: strings.Split(os.Getenv("REDIS_ADDR"), ","),
	})
	win := Window{
		rds: cli,
	}

	err := win.SetBlock(context.TODO(), key, time.Second*100)
	if err != nil {
		t.Fatal(err)
	}

	err = win.AddBlockTime(context.TODO(), key, time.Second*100)
	if err != nil {
		t.Fatal(err)
	}

	err = win.AddBlockTime(context.TODO(), key, -time.Second*50)
	if err != nil {
		t.Fatal(err)
	}

	ttl, err := win.GetBlockTTL(context.TODO(), key)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != time.Second*150 {
		t.Fatal("ttl should be equal 150s")
	}

	err = win.ClearBlockStatus(context.TODO(), key)
	if err != nil {
		t.Fatal(err)
	}
}
