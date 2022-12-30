package test

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/queue"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPush(t *testing.T) {
	fs.Initialize[queue.Module]("unit test")
	queue.MoveQueueInterval = 100 * time.Millisecond

	assert.Panics(t, func() {
		queue.Push("test", 0)
	})
	var aSum int
	queue.Subscribe("test", "A", 2, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "A", subscribeName)
		var lst collections.List[int]
		lstMessage.MapToList(&lst)
		aSum += lst.SumItem()
	})

	var bSum int
	queue.Subscribe("test", "B", 4, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "B", subscribeName)
		var lst collections.List[int]
		lstMessage.MapToList(&lst)
		bSum += lst.SumItem()
	})

	queue.Subscribe("test", "C", 100, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		panic("测试panic")
	})

	time.Sleep(550 * time.Millisecond)
	for i := 0; i < 100; i++ {
		queue.Push("test", i)
		time.Sleep(1 * time.Microsecond)
	}

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 4950, aSum)
	assert.Equal(t, 4950, bSum)
	//flog.Info("finish")
}
