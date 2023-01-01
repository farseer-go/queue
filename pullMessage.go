package queue

import (
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/stopwatch"
)

// 每个订阅者独立消费
func (curSubscriber *subscriber) pullMessage() {
	for {
		// 如果未消费的长度小于1，则说明没有新的数据
		if !curSubscriber.isHaveMessage() {
			<-curSubscriber.notify
			// 如果通知里还有数据，则清空
			for len(curSubscriber.notify) > 0 {
				<-curSubscriber.notify
			}
		}

		// 得出未消费的长度
		pullCount := curSubscriber.getPullCount()

		// 设置为消费中
		curSubscriber.queueManager.work()

		// 计算当前订阅者应消费队列的起始位置
		startIndex := curSubscriber.offset + 1
		endIndex := startIndex + pullCount

		// 得到本次消费的队列切片
		curQueue := curSubscriber.queueManager.queue.Range(startIndex, pullCount).ToListAny()
		remainingCount := curSubscriber.queueManager.queue.Count() - endIndex

		// 执行客户端的消费
		exception.Try(func() {
			sw := stopwatch.StartNew()
			curSubscriber.subscribeFunc(curSubscriber.subscribeName, curQueue, remainingCount)
			flog.ComponentInfof("queue", "Subscribe：%s，PullCount：%d，ElapsedTime：%s", curSubscriber.subscribeName, pullCount, sw.GetMillisecondsText())
		}).CatchException(func(exp any) {
			_ = flog.Error(exp)
		})

		// 保存本次消费的位置
		curSubscriber.offset = endIndex - 1

		curSubscriber.queueManager.unWork()
	}
}

// 是否有新的消息
func (curSubscriber *subscriber) isHaveMessage() bool {
	curSubscriber.queueManager.work()
	defer curSubscriber.queueManager.unWork()

	return curSubscriber.queueManager.queue.Count()-curSubscriber.offset-1 > 0
}

// 计算本次可以消费的数量
func (curSubscriber *subscriber) getPullCount() int {
	curSubscriber.queueManager.work()
	defer curSubscriber.queueManager.unWork()

	pullCount := curSubscriber.queueManager.queue.Count() - curSubscriber.offset - 1
	// 如果超出每次拉取的数量，则以拉取设置为准
	if pullCount > curSubscriber.pullCount {
		pullCount = curSubscriber.pullCount
	}
	return pullCount
}
