package test

import (
	"sync"
	"testing"
	"time"

	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/queue"
	"github.com/stretchr/testify/assert"
)

func TestPush(t *testing.T) {
	fs.Initialize[queue.Module]("unit test")
	queue.MoveQueueInterval = 100 * time.Millisecond

	assert.Panics(t, func() {
		queue.Push("test", 0)
	})
	var aSum int
	var lockA sync.Mutex
	queue.Subscribe("test", "A", 2, 0, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "A", subscribeName)
		lst := collections.NewList[int]()
		lstMessage.Foreach(func(item *any) {
			lst.Add((*item).(int))
		})

		lockA.Lock()
		defer lockA.Unlock()
		aSum += lst.SumItem()
	})

	var bSum int
	var lockB sync.Mutex
	queue.Subscribe("test", "B", 4, 0, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "B", subscribeName)
		lst := collections.NewList[int]()
		lstMessage.Foreach(func(item *any) {
			lst.Add((*item).(int))
		})

		lockB.Lock()
		defer lockB.Unlock()
		bSum += lst.SumItem()
	})

	queue.Subscribe("test", "C", 100, 0, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		panic("测试panic")
	})

	time.Sleep(550 * time.Millisecond)
	for i := 0; i < 100; i++ {
		queue.Push("test", i)
		time.Sleep(1 * time.Microsecond)
	}

	time.Sleep(200 * time.Millisecond)

	lockA.Lock()
	lockB.Lock()
	defer lockA.Unlock()
	defer lockB.Unlock()
	assert.Equal(t, 4950, aSum)
	assert.Equal(t, 4950, bSum)
}
