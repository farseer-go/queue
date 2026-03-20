package queue

import (
	"sync"
	"time"

	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/asyncLocal"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/trace"
)

// Consumer 消费
type queueSubscribeFunc func(subscribeName string, lstMessage collections.ListAny, remainingCount int)

// subscribeMu 保护 dicQueue 的写操作，避免并发 Subscribe 同名队列时创建重复 manager（竞态 Bug Fix）
var subscribeMu sync.Mutex

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
	// Perf Fix: buffer=1 足够唤醒协程，100000 仅浪费内存
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
	// Bug Fix: 用互斥锁保证 ContainsKey + Add 的原子性
	// 原代码两次操作之间没有保护，并发调用会创建重复 manager
	subscribeMu.Lock()
	if !dicQueue.ContainsKey(queueName) {
		manager := newQueueManager(queueName)
		go manager.stat()
		dicQueue.Add(queueName, manager)
	}
	queue := dicQueue.GetValue(queueName)
	subscribeMu.Unlock()

	// 添加订阅者
	sub := &subscriber{
		subscribeName: subscribeName,
		offset:        -1,
		subscribeFunc: fn,
		pullCount:     pullCount,
		sleepTime:     sleepTime,
		queueManager:  queue,
		notify:        make(chan bool, 1),
		traceManager:  container.Resolve[trace.IManager](),
	}

	// Bug Fix: subscribers.Add 必须在 queueLock 保护下执行
	// stat() 在同一把锁内遍历 subscribers，不加锁会有数据竞争
	queue.queueLock.Lock()
	queue.subscribers.Add(sub)
	queue.queueLock.Unlock()

	go sub.pullMessage()
}

// 计算本次可以消费的数量
// 注意：调用方须在 queueLock 保护下调用，避免 moveQueue 并发修改 queue 导致脏读
func (receiver *subscriber) getPullCount() int {
	pullCount := receiver.queueManager.queue.Count() - receiver.offset - 1
	// 如果超出每次拉取的数量，则以拉取设置为准
	if receiver.pullCount > 0 && pullCount > receiver.pullCount {
		pullCount = receiver.pullCount
	}
	return pullCount
}

// 每个订阅者独立消费
func (receiver *subscriber) pullMessage() {
	for {
		// ── 加锁阶段：仅保护队列切片读取，不持锁执行回调 ──────────────────
		receiver.queueManager.queueLock.Lock()

		// 排空积压通知
		for len(receiver.notify) > 0 {
			<-receiver.notify
		}

		// Bug Fix: 在锁内重新计算 pullCount
		// 原代码在锁外调用 getPullCount()，moveQueue 可能已压缩队列导致索引错位
		pullCount := receiver.getPullCount()
		if pullCount < 1 {
			// 没有新数据：先释放锁，再阻塞等待通知
			receiver.queueManager.queueLock.Unlock()
			<-receiver.notify
			continue
		}

		startIndex := receiver.offset + 1
		endIndex := startIndex + pullCount

		// 复制本次消费的数据切片（此后无需持锁）
		curQueue := receiver.queueManager.queue.Range(startIndex, pullCount).ToListAny()
		remainingCount := receiver.queueManager.queue.Count() - endIndex

		// Perf Fix: 锁在此处释放，回调执行期间 Push 不再被阻塞
		receiver.queueManager.queueLock.Unlock()
		// ─────────────────────────────────────────────────────────────────

		// Bug Fix: asyncLocal/trace 初始化移至锁外
		// 原代码在 Lock 之后、Try 之外调用，panic 会导致锁永久泄漏
		asyncLocal.InitContext()
		traceContext := receiver.traceManager.EntryQueueConsumer(receiver.queueManager.name, receiver.subscribeName)

		exception.Try(func() {
			receiver.subscribeFunc(receiver.subscribeName, curQueue, remainingCount)

			// 成功后才推进 offset（retry 语义：失败时下次重新消费同批消息）
			receiver.queueManager.queueLock.Lock()
			receiver.offset += pullCount
			receiver.queueManager.queueLock.Unlock()
		}).CatchException(func(exp any) {
			<-time.After(time.Second)
		})

		// Perf Fix: 复用 receiver.traceManager，避免每次循环重新 Resolve
		receiver.traceManager.Push(traceContext, nil)
		asyncLocal.Release()

		// 休眠指定时间
		if receiver.sleepTime > 0 {
			time.Sleep(receiver.sleepTime)
		}
	}
}
