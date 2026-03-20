package queue

// Push 添加数据到队列中
func Push(queueName string, message any) {
	// 首先从订阅者中找到是否存在queueName
	if !dicQueue.ContainsKey(queueName) {
		panic("未找到队列名称：" + queueName + "，需要先通过订阅队列后，才能Push数据，以防止内存泄露。")
	}

	// 添加数据到队列
	queueList := dicQueue.GetValue(queueName)

	queueList.queueLock.Lock()
	queueList.queue.Add(message)
	// 在锁内获取订阅者快照，避免与 Subscribe() 的 subscribers.Add 并发竞争
	subscribers := queueList.subscribers.ToArray()
	queueList.queueLock.Unlock()

	// Bug Fix: 改为非阻塞发送
	// 原代码 notify <- true 是阻塞发送：当 buffer=1 且并发 Push 时会卡住调用方
	// 若 channel 已有待消费通知，subscriber 醒来后会自行判断 pullCount，无需重复发送
	for _, sub := range subscribers {
		select {
		case sub.notify <- true:
		default:
		}
	}
}
