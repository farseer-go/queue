package queue

import (
	"sync"
	"time"

	"github.com/farseer-go/collections"
)

// 队列
// key = name
// value = 队列
var dicQueue collections.Dictionary[string, *queueManager]

// 队列列表
type queueManager struct {
	// 当前队列名称
	name string
	// 全局队列
	queue collections.ListAny
	// 当前消费到的索引位置（如果是多个消费者，只记录最早的索引位置）
	// 用于定时移除queue已被消费的数据，以节省内存空间
	minOffset int
	// 订阅者
	subscribers collections.List[*subscriber]
	// 读写锁
	queueLock sync.RWMutex
}

func newQueueManager(queueName string) *queueManager {
	return &queueManager{
		name:        queueName,
		minOffset:   -1,
		queue:       collections.NewListAny(),
		subscribers: collections.NewList[*subscriber](),
	}
}

// 定时检查一下队列的消费长度
func (receiver *queueManager) stat() {
	for {
		time.Sleep(MoveQueueInterval)
		// 得到当前所有订阅者的最后消费的位置的最小值
		receiver.statLastIndex()

		// 所有订阅者没有在执行的时候，做一次队列合并
		if receiver.minOffset > -1 {
			receiver.queueLock.Lock()
			receiver.moveQueue()
			receiver.queueLock.Unlock()
		}
	}
}

// 得到当前所有订阅者的最后消费的位置的最小值
func (queueList *queueManager) statLastIndex() {
	// 计算当前所有订阅者的最后消费的位置的最小值
	if queueList.subscribers.Count() > 0 {
		queueList.minOffset = queueList.subscribers.Min(func(item *subscriber) any {
			//return atomic.LoadInt64(&item.offset)
			return item.offset
		}).(int)
	}
}

// 缩减使用过的队列
func (receiver *queueManager) moveQueue() {
	// 裁剪队列，将头部已消费的移除
	arr := receiver.queue.RangeStart(int(receiver.minOffset + 1)).ToArray()
	receiver.queue = collections.NewListAny(arr...)

	// 设置每个订阅者的偏移量
	for i := 0; i < receiver.subscribers.Count(); i++ {
		//atomic.AddInt64(&receiver.subscribers.Index(i).offset, -int64(receiver.minOffset)-1)
		receiver.subscribers.Index(i).offset -= receiver.minOffset + 1
	}
}
