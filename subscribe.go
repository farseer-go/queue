package queue

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/trace"
)

// 订阅者的队列
type subscriber struct {
	// 订阅者名称
	subscribeName string
	// 最后消费的位置
	offset int
	// 订阅者
	subscribeFunc queueSubscribeFunc
	// 每次拉取的数量
	pullCount int
	// 所属的队列
	queueManager *queueManager
	// 等待通知有新的消息
	notify chan bool
	// 链路追踪
	traceManager trace.IManager
}

func newSubscriber(subscribeName string, fn queueSubscribeFunc, pullCount int, queue *queueManager) *subscriber {
	return &subscriber{
		subscribeName: subscribeName,
		offset:        -1,
		subscribeFunc: fn,
		pullCount:     pullCount,
		queueManager:  queue,
		notify:        make(chan bool, 100000),
		traceManager:  container.Resolve[trace.IManager](),
	}
}

// Consumer 消费
type queueSubscribeFunc func(subscribeName string, lstMessage collections.ListAny, remainingCount int)

// Subscribe 订阅消息
// queueName = 队列名称
// subscribeName = 订阅者名称
// pullCount = 每次拉取的数量
// queueSubscribeFunc = 消费逻辑
func Subscribe(queueName string, subscribeName string, pullCount int, fn queueSubscribeFunc) {
	// 判断队列中是否有queueName这个队列，如果没有，则创建这个队列
	if !dicQueue.ContainsKey(queueName) {
		manager := newQueueManager(queueName)
		go manager.stat()
		dicQueue.Add(queueName, manager)
	}

	// 找到对应的队列
	queue := dicQueue.GetValue(queueName)

	// 添加订阅者
	subscriber := newSubscriber(subscribeName, fn, pullCount, queue)
	queue.subscribers.Add(subscriber)

	go subscriber.pullMessage()
}
