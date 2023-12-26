package queue

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/flog"
	"sync"
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
	// 读写锁
	lock sync.RWMutex
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
func (queueList *queueManager) stat() {
	for {
		time.Sleep(MoveQueueInterval)
		queueList.lock.Lock()

		// 得到当前所有订阅者的最后消费的位置的最小值
		queueList.statLastIndex()

		// 所有订阅者没有在执行的时候，做一次队列合并
		if queueList.minOffset > -1 {
			preLength := queueList.queue.Count()
			queueList.moveQueue()
			flog.ComponentInfof("queue", "Migrating Data，QueueName：%s，queueLength：%d -> %d", queueList.name, preLength, queueList.queue.Count())
		}
		queueList.lock.Unlock()
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
	// 裁剪队列，将头部已消费的移除
	queueList.queue = queueList.queue.RangeStart(queueList.minOffset + 1).ToListAny()

	// 设置每个订阅者的偏移量
	for i := 0; i < queueList.subscribers.Count(); i++ {
		queueList.subscribers.Index(i).offset -= queueList.minOffset + 1
	}
}

// 消费中
func (queueList *queueManager) work() {
	queueList.lock.RLock()
}

// 消费完毕
func (queueList *queueManager) unWork() {
	queueList.lock.RUnlock()
}
