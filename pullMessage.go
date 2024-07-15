package queue

import (
	"github.com/farseer-go/fs/asyncLocal"
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/stopwatch"
	"time"
)

// 每个订阅者独立消费
func (receiver *subscriber) pullMessage() {
	for {
		// 如果未消费的长度小于1，则说明没有新的数据
		if !receiver.isHaveMessage() {
			//fmt.Printf("队列名：%s，订阅者：%s，没有数据，等待通知\n", receiver.queueManager.name, receiver.subscribeName)
			<-receiver.notify
		}

		// 得出未消费的长度
		pullCount := receiver.getPullCount()
		//fmt.Printf("队列名：%s，订阅者：%s，数据来了，共%d条\n", receiver.queueManager.name, receiver.subscribeName, pullCount)

		// 本次消费长度为0，跳出
		if pullCount == 0 {
			continue
		}

		receiver.notify = make(chan bool, 100000)
		// 设置为消费中
		receiver.queueManager.work()

		// 计算当前订阅者应消费队列的起始位置
		startIndex := receiver.offset + 1
		endIndex := startIndex + pullCount

		// 得到本次消费的队列切片
		curQueue := receiver.queueManager.queue.Range(startIndex, pullCount).ToListAny()
		remainingCount := receiver.queueManager.queue.Count() - endIndex

		traceContext := receiver.traceManager.EntryQueueConsumer(receiver.queueManager.name, receiver.subscribeName)
		// 执行客户端的消费
		exception.Try(func() {
			sw := stopwatch.StartNew()
			receiver.subscribeFunc(receiver.subscribeName, curQueue, remainingCount)
			// 保存本次消费的位置
			receiver.offset = endIndex - 1
			flog.ComponentInfof("queue", "Subscribe：%s，PullCount：%d，ElapsedTime：%s", receiver.subscribeName, pullCount, sw.GetMillisecondsText())
		}).CatchException(func(exp any) {
			traceContext.Error(flog.Error(exp))
			<-time.After(time.Second)
		})
		traceContext.End()
		asyncLocal.Release()

		receiver.queueManager.unWork()

		// 休眠指定时间
		if receiver.sleepTime > 0 {
			time.Sleep(receiver.sleepTime)
		}
	}
}

// 是否有新的消息
func (receiver *subscriber) isHaveMessage() bool {
	receiver.queueManager.work()
	defer receiver.queueManager.unWork()

	return receiver.queueManager.queue.Count()-receiver.offset-1 > 0
}

// 计算本次可以消费的数量
func (receiver *subscriber) getPullCount() int {
	receiver.queueManager.work()
	defer receiver.queueManager.unWork()

	pullCount := receiver.queueManager.queue.Count() - receiver.offset - 1
	// 如果超出每次拉取的数量，则以拉取设置为准
	if pullCount > receiver.pullCount {
		pullCount = receiver.pullCount
	}
	return pullCount
}
