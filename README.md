# queue 本地队列
> 包：`"github.com/farseer-go/queue"`
>
> 模块：`queue.Module`

- `Document`
    - [English](https://farseer-go.github.io/doc/#/en-us/)
    - [中文](https://farseer-go.github.io/doc/)
    - [English](https://farseer-go.github.io/doc/#/en-us/)
- Source
    - [github](https://github.com/farseer-go/fs)


![](https://img.shields.io/github/stars/farseer-go?style=social)
![](https://img.shields.io/github/license/farseer-go/queue)
![](https://img.shields.io/github/go-mod/go-version/farseer-go/queue)
![](https://img.shields.io/github/v/release/farseer-go/queue)
[![codecov](https://img.shields.io/codecov/c/github/farseer-go/queue)](https://codecov.io/gh/farseer-go/queue)
![](https://img.shields.io/github/languages/code-size/farseer-go/queue)
[![Build](https://github.com/farseer-go/queue/actions/workflows/test.yml/badge.svg)](https://github.com/farseer-go/queue/actions/workflows/test.yml)
![](https://goreportcard.com/badge/github.com/farseer-go/queue)

## 概述
在我们的生产环境中，我们每秒钟、每分钟都会产生非常多的执行日志。

如果每个产生的日志都被写到ES、数据库一次，就会给IO带来更大的压力。

而这些日志数据在一定程度上是允许被延迟的。

理想的做法是积累到一定时间或一定数量，然后再批量写入。

这时使用本地队列是最合适的，因为它足够轻量，不需要搭建服务端的消息队列中间件。

## 1、生产消息
本着farseer-go极简、优雅风格，使用queue组件也是非常简单的：
```go
func Push(queueName string, message any)
```
- `queueName`：队列名称
- `message`：消息内容

_演示：_
```go
func main() {
    fs.Initialize[queue.Module]("queue生产消息演示")

    for i := 0; i < 100; i++ {
        queue.Push("test", i)
    }
}
```

## 2、消费
_函数定义：_
```go
type queueSubscribeFunc func(subscribeName string, lstMessage collections.ListAny, remainingCount int)
func Subscribe(queueName string, subscribeName string, pullCount int, fn queueSubscribeFunc)
```
- `queueName`：队列名称
- `subscribeName`：订阅名称
- `pullCount`：每次拉取的数量
- `fn`：回调函数
- `lstMessage`：本次拉取消息的集合
- `remainingCount`：队列中剩余的数量

> 如果有多个不同的`subscribeName`订阅者，`订阅同一个队列`时，则他们的`消费是独立`的，互相不会影响彼此的进度。

_演示：_
```go
func main() {
    fs.Initialize[queue.Module]("queue生产消息演示")

    // 消费test队列，每次只拉2条记录
    queue.Subscribe("test", "A", 2, consumer)

    // 消费逻辑
    func consumer(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
        var lst collections.List[int]
        lstMessage.MapToList(&lst)
        flog.Info(lst.Count())
    }
}
```