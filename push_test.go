package queue

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/modules"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testSubscribe struct {
	lst collections.List[int]
}

func TestPush(t *testing.T) {
	modules.StartModules(Module{})

	var aSum int
	Subscribe("test", "A", 2, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "A", subscribeName)
		var lst collections.List[int]
		lstMessage.MapToList(&lst)
		aSum += lst.SumItem()
	})

	var bSum int
	Subscribe("test", "B", 4, func(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
		assert.Equal(t, "B", subscribeName)
		var lst collections.List[int]
		lstMessage.MapToList(&lst)
		bSum += lst.SumItem()
	})

	queue := dicQueue.GetValue("test")
	assert.Equal(t, 2, queue.subscribers.Count())

	for i := 0; i < 100; i++ {
		Push("test", i)
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 4950, aSum)
	assert.Equal(t, 4950, bSum)
	time.Sleep(6 * time.Second)
	assert.Equal(t, -1, queue.minOffset)
	assert.True(t, queue.queue.IsEmpty())
}
