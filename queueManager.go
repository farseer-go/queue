package queue

import (
	"github.com/farseer-go/collections"
	"time"
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
	// 是否在迁移队列
	isMoveQueue bool
}

func newQueueManager(queueName string) *queueManager {
	return &queueManager{
		name:        queueName,
		minOffset:   -1,
		queue:       collections.NewListAny(),
		subscribers: collections.NewList[*subscriber](),
	}
}

// 得到当前所有订阅者的最后消费的位置的最小值
func (queueList *queueManager) statLastIndex() {
	if queueList.subscribers.Count() > 0 {
		queueList.minOffset = queueList.subscribers.Min(func(item *subscriber) any {
			return item.offset
		}).(int)
	}
}

// 缩减使用过的队列
func (queueList *queueManager) moveQueue() {
	// 没有使用，则不缩减
	if queueList.minOffset < 0 {
		return
	}

	// 缩减队列
	queueList.minOffset += 1

	// 裁剪队列，将头部已消费的移除
	queueList.queue = queueList.queue.RangeStart(queueList.minOffset).ToListAny()
	// 设置每个订阅者的偏移量
	for _, subscriberQueue := range queueList.subscribers.ToArray() {
		subscriberQueue.offset -= queueList.minOffset
	}
	queueList.minOffset = -1
}

// 1分钟检查一下队列的消费长度
func (queueList *queueManager) stat() {
	for {
		time.Sleep(5 * time.Second)
		queueList.statLastIndex()

		// 所有订阅者没有在执行的时候，做一次队列合并
		if queueList.minOffset > -1 && queueList.subscribers.All(func(item *subscriber) bool {
			return !item.isWork && len(item.notify) == 0
		}) {
			queueList.isMoveQueue = true
			queueList.moveQueue()
			queueList.isMoveQueue = false
		}
	}
}
