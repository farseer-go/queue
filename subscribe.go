package queue

import (
	"time"

	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/asyncLocal"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/trace"
)

// Consumer 消费
type queueSubscribeFunc func(subscribeName string, lstMessage collections.ListAny, remainingCount int)

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
	// 休眠时间
	sleepTime time.Duration
}

// Subscribe 订阅消息
// queueName = 队列名称
// subscribeName = 订阅者名称
// pullCount = 每次拉取的数量
// queueSubscribeFunc = 消费逻辑
func Subscribe(queueName string, subscribeName string, pullCount int, sleepTime time.Duration, fn queueSubscribeFunc) {
	// 判断队列中是否有queueName这个队列，如果没有，则创建这个队列
	if !dicQueue.ContainsKey(queueName) {
		manager := newQueueManager(queueName)
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
		sleepTime:     sleepTime,
		queueManager:  queue,
		notify:        make(chan bool, 100000),
		traceManager:  container.Resolve[trace.IManager](),
	}

	queue.subscribers.Add(subscriber)
	go subscriber.pullMessage()
}

// 计算本次可以消费的数量
func (receiver *subscriber) getPullCount() int {
	pullCount := receiver.queueManager.queue.Count() - int(receiver.offset) - 1
	// 如果超出每次拉取的数量，则以拉取设置为准
	if receiver.pullCount > 0 && pullCount > receiver.pullCount {
		pullCount = receiver.pullCount
	}
	return pullCount
}

// 每个订阅者独立消费
func (receiver *subscriber) pullMessage() {
	for {
		// 得出未消费的长度
		pullCount := receiver.getPullCount()
		// 如果未消费的长度小于1，则说明没有新的数据
		if pullCount < 1 {
			<-receiver.notify
			continue
		}

		// 设置为消费中
		receiver.queueManager.queueLock.Lock()

		//receiver.notify = make(chan bool, 100000)
		for len(receiver.notify) > 0 {
			<-receiver.notify
		}

		// 计算当前订阅者应消费队列的起始位置
		startIndex := receiver.offset + 1
		endIndex := startIndex + pullCount

		// 得到本次消费的队列切片
		curQueue := receiver.queueManager.queue.Range(startIndex, pullCount).ToListAny()
		remainingCount := receiver.queueManager.queue.Count() - endIndex

		// InitContext 初始化同一协程上下文，避免在同一协程中多次初始化
		asyncLocal.InitContext()
		traceContext := receiver.traceManager.EntryQueueConsumer(receiver.queueManager.name, receiver.subscribeName)
		// 执行客户端的消费
		exception.Try(func() {
			receiver.subscribeFunc(receiver.subscribeName, curQueue, remainingCount)
			// 保存本次消费的位置
			//atomic.StoreInt64(&receiver.offset, int64(endIndex-1))
			receiver.offset += pullCount
			receiver.queueManager.queueLock.Unlock()
		}).CatchException(func(exp any) {
			receiver.queueManager.queueLock.Unlock()
			<-time.After(time.Second)
		})

		curQueue.Clear()
		container.Resolve[trace.IManager]().Push(traceContext, nil)
		asyncLocal.Release()

		// 休眠指定时间
		if receiver.sleepTime > 0 {
			time.Sleep(receiver.sleepTime)
		}
	}
}
