package test

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/queue"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testSubscribe struct {
	lst collections.List[int]
}

func TestPush(t *testing.T) {
	fs.Initialize[queue.Module]("unit test")

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

	// todo：暂时注释
	//myQueue := queue.dicQueue.GetValue("test")
	//assert.Equal(t, 2, myQueue.subscribers.Count())
	//
	//for i := 0; i < 100; i++ {
	//	queue.Push("test", i)
	//}
	//
	//time.Sleep(100 * time.Millisecond)
	//assert.Equal(t, 4950, aSum)
	//assert.Equal(t, 4950, bSum)
	//time.Sleep(6 * time.Second)
	//assert.Equal(t, -1, myQueue.minOffset)
	//assert.True(t, myQueue.queue.IsEmpty())
}
