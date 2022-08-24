### Local Queue Usage Scenarios
We generate a very large number of execution logs per second, per minute in our production environment.

If every log generated is written to ES, database once, it will put more pressure on IO.

And this log data is allowed to be delayed to some extent.

The ideal practice is to accumulate to a certain time, or quantity, and then write in bulk.

## What are the functions?
* queue
    * func
        * Push （product message）
        * Subscribe （subscribe queue）


## Getting Started
Subscribe Queue
```go
// consumerFunc
// subscribeName = your custom,this is A
// lstMessage = pull collection list
// remainingCount = Number of remaining unconsumed in the queue
func consumer(subscribeName string, lstMessage collections.ListAny, remainingCount int) {
    var lst collections.List[int]
    lstMessage.MapToList(&lst)
    
    lst.Count() // return 2
}

// Subscribe
// "test" = QueueName
// "A" = SubscribeName
// 2 = Number of pulls
queue.Subscribe("test", "A", 2, consumer)


```
Product message
```go
for i := 0; i < 100; i++ {
    // "test" = QueueName
	// i = message
    queue.Push("test", i)
}
```