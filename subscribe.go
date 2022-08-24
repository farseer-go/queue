package queue

import "github.com/farseer-go/collections"

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
	// 是否正在执行消费中
	isWork bool
	// 等待通知有新的消息
	notify chan bool
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
		manager := &queueManager{
			name:        queueName,
			minOffset:   -1,
			queue:       collections.NewListAny(),
			subscribers: collections.NewList[*subscriber](),
		}
		go manager.stat()
		dicQueue.Add(queueName, manager)
	}

	// 找到对应的队列
	queue := dicQueue.GetValue(queueName)

	// 添加订阅者
	subscriber := &subscriber{
		subscribeName: subscribeName,
		offset:        -1,
		subscribeFunc: fn,
		pullCount:     pullCount,
		queueManager:  queue,
		notify:        make(chan bool, 100000),
	}
	queue.subscribers.Add(subscriber)

	go subscriber.pullMessage()
}
