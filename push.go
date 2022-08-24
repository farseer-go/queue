package queue

// Push 添加数据到队列中
func Push(queueName string, message any) {
	// 首先从订阅者中找到是否存在eventName
	if !dicQueue.ContainsKey(queueName) {
		panic("未找到队列名称：" + queueName + "，需要先通过订阅队列后，才能Push数据")
	}

	// 添加数据到队列
	queueList := dicQueue.GetValue(queueName)
	queueList.queue.Add(message)

	// 未执行中订阅者，发送有新消息通知
	go func() {
		sleepSubscribers := queueList.subscribers.Where(func(item *subscriber) bool {
			return !item.isWork
		}).ToArray()

		// 通知有新的消息
		for _, sleepSubscriber := range sleepSubscribers {
			sleepSubscriber.notify <- true
		}
	}()
}
