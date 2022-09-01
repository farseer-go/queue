package queue

import (
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/flog"
	"time"
)

// 每个订阅者独立消费
func (curSubscriber *subscriber) pullMessage() {
	for {
		// 如果未消费的长度小于1，则说明没有新的数据
		if !curSubscriber.isHaveMessage() {
			select {
			case <-curSubscriber.notify:
				// 如果通知里还有数据，则清空
				for len(curSubscriber.notify) > 0 {
					<-curSubscriber.notify
				}
			}
		}

		// 得出未消费的长度
		unConsumerLength := curSubscriber.queueManager.queue.Count() - curSubscriber.offset - 1
		if unConsumerLength < 1 {
			continue
		}

		// 设置为消费中
		curSubscriber.isWork = true

		// 正在迁移队列，这时不能消费（要在isWork=true之后判断）
		if curSubscriber.queueManager.isMoveQueue {
			time.Sleep(10 * time.Millisecond)
			curSubscriber.isWork = false
			continue
		}

		if unConsumerLength < curSubscriber.pullCount {
			// 小于预期的设置时，等待100ms
			time.Sleep(100 * time.Millisecond)
			unConsumerLength = curSubscriber.queueManager.queue.Count() - curSubscriber.offset - 1
		}

		// 计算本次应拉取的数量
		if unConsumerLength > curSubscriber.pullCount {
			unConsumerLength = curSubscriber.pullCount
		}

		// 计算当前订阅者应消费队列的起始位置
		startIndex := curSubscriber.offset + 1
		endIndex := startIndex + unConsumerLength

		// 得到本次消费的队列切片
		curQueue := curSubscriber.queueManager.queue.Range(startIndex, unConsumerLength).ToListAny()
		remainingCount := curSubscriber.queueManager.queue.Count() - endIndex

		// 执行客户端的消费
		try := exception.Try(func() {
			curSubscriber.subscribeFunc(curSubscriber.subscribeName, curQueue, remainingCount)
			// 保存本次消费的位置
			curSubscriber.offset = endIndex - 1
		})
		try.CatchException(func(exp any) {
			flog.Error(exp)
		})

		curSubscriber.isWork = false
	}
}

// 是否有新的消息
func (curSubscriber *subscriber) isHaveMessage() bool {
	return curSubscriber.queueManager.queue.Count()-curSubscriber.offset-1 > 0
}
