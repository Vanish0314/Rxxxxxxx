# Rxxxxxxx

# 示例

## 调度调度器的代码

本示例展示 Rx.NET 中 SubscribeOn 与 Interval 调度器的关系：
- **Scenario A** 使用 ImmediateScheduler，Tick 在 EventLoopScheduler 线程执行
- **Scenario B** 使用默认线程池，Tick 在 ThreadPool 执行

### 示例代码
```c#
using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;

// 场景A：Interval 使用 ImmediateScheduler → 由 SubscribeOn 的 EventLoopScheduler 线程来执行 Tick
Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] Main thread - Scenario A start");

Observable
    .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
    .SubscribeOn(
        new EventLoopScheduler(
            (start) =>
            {
                Thread t = new(start) { IsBackground = false };
                Console.WriteLine($"[T:{t.ManagedThreadId}] Created thread for EventLoopScheduler");
                return t;
            }
        )
    )
    .Subscribe(tick =>
        Console.WriteLine(
            $"[A][T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Tick {tick}"
        )
    );

Console.WriteLine(
    $"[T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Scenario A subscribed"
);

// 场景B：Interval 使用默认调度器（线程池）→ Tick 在线程池线程上执行，即使 SubscribeOn 指定了 EventLoopScheduler
// 原因：SubscribeOn 影响的是“订阅”动作在哪个调度器上进行，Interval 的“生产（Tick）”由其自己的调度器决定
Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] Main thread - Scenario B start");

Observable
    .Interval(TimeSpan.FromSeconds(1))
    .SubscribeOn(
        new EventLoopScheduler(
            (start) =>
            {
                Thread t = new(start) { IsBackground = false };
                Console.WriteLine($"[T:{t.ManagedThreadId}] Created thread for EventLoopScheduler");
                return t;
            }
        )
    )
    .Subscribe(tick =>
        Console.WriteLine(
            $"[B][T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Tick {tick}"
        )
    );

Console.WriteLine(
    $"[T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Scenario B subscribed"
);

// 结论说明：
// - 使用 EventLoopScheduler 进行 SubscribeOn：订阅发生在它创建的线程上
// - Interval 默认使用线程池调度器：Tick 出现在线程池（你会看到与 EventLoop 线程不同的线程Id）
// - 若 Interval 显式使用 ImmediateScheduler：Tick 会在当前上下文（这里是 EventLoopScheduler 线程）执行

Console.WriteLine(
    $"[T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Main thread exiting (process stays alive due to non-background EventLoop thread)"
);
```

### 输出
```
[T:1] Main thread - Scenario A start
[T:5] Created thread for EventLoopScheduler
[T:1] 09/17/2025 14:24:34: Scenario A subscribed
[T:1] Main thread - Scenario B start
[T:6] Created thread for EventLoopScheduler
[T:1] 09/17/2025 14:24:34: Scenario B subscribed
[T:1] 09/17/2025 14:24:34: Main thread exiting (process stays alive due to non-background EventLoop thread)
[A][T:5] 09/17/2025 14:24:35: Tick 0
[B][T:7] 09/17/2025 14:24:35: Tick 0
[A][T:5] 09/17/2025 14:24:36: Tick 1
[B][T:7] 09/17/2025 14:24:36: Tick 1
[A][T:5] 09/17/2025 14:24:37: Tick 2
[B][T:7] 09/17/2025 14:24:37: Tick 2
[A][T:5] 09/17/2025 14:24:38: Tick 3
[B][T:7] 09/17/2025 14:24:38: Tick 3
[A][T:5] 09/17/2025 14:24:39: Tick 4
[B][T:7] 09/17/2025 14:24:39: Tick 4
[A][T:5] 09/17/2025 14:24:40: Tick 5
[B][T:7] 09/17/2025 14:24:40: Tick 5
[A][T:5] 09/17/2025 14:24:41: Tick 6
[B][T:7] 09/17/2025 14:24:41: Tick 6
[A][T:5] 09/17/2025 14:24:42: Tick 7
[B][T:7] 09/17/2025 14:24:42: Tick 7
[A][T:5] 09/17/2025 14:24:43: Tick 8
[B][T:7] 09/17/2025 14:24:43: Tick 8
```

## Delay操作符不延迟订阅而是延迟转发
### 示例代码
```c#
﻿using System.Reactive;
using System.Reactive.Linq;

IObservable<Timestamped<long>> source = Observable
    .Interval(TimeSpan.FromSeconds(1))
    .Take(5)
    .Timestamp();

IObservable<Timestamped<long>> delay = source.Delay(TimeSpan.FromSeconds(2));

delay.Subscribe(
    value =>
        Console.WriteLine(
            $"Item {value.Value} with timestamp {value.Timestamp} received at {DateTimeOffset.Now}"
        ),
    () => Console.WriteLine("delay Completed")
);

Console.WriteLine("Press Enter to exit");
Console.ReadLine();
```

### 输出
```
Press Enter to exit
```

## Throttle操作符
### 示例代码
```c#
﻿using System.Reactive;
using System.Reactive.Linq;

var source = Observable.Create<int>(observer =>
{
    // 模拟快速连续发射事件
    observer.OnNext(1);
    Thread.Sleep(200);
    observer.OnNext(2);
    Thread.Sleep(200);
    observer.OnNext(3);
    Thread.Sleep(1200); // 这里有足够的间隔
    observer.OnNext(4);
    observer.OnCompleted();
    return () => { };
});

source
    .Throttle(TimeSpan.FromSeconds(1))
    .Subscribe(x => Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} -> {x}"));

Thread.Sleep(3000); // 保证序列执行完
```

### 输出
```
09:09:51.664 -> 3
09:09:51.862 -> 4
```

## Catch操作符
### 示例代码
```c#
﻿using System.Reactive.Linq;

// 模拟一个可能抛出异常的 Observable
var source = Observable.Create<int>(observer =>
{
    observer.OnNext(1);
    observer.OnNext(2);
    observer.OnNext(3);
    observer.OnError(new Exception("发生错误！"));
    observer.OnNext(4); // 不会被执行
    return System.Reactive.Disposables.Disposable.Empty;
});

// 使用 Catch 捕获异常
var handled = source.Catch<int, Exception>(ex =>
{
    Console.WriteLine($"捕获异常: {ex.Message}");
    // 提供备用 Observable
    return Observable.Return(999);
});

handled.Subscribe(
    x => Console.WriteLine($"接收到: {x}"),
    ex => Console.WriteLine($"流终止: {ex.Message}"),
    () => Console.WriteLine("流完成")
);

var first = Observable.Throw<int>(new Exception("第一个流出错"));
var second = Observable.Return(42);

var result = first.Catch(second);

result.Subscribe(
    x => Console.WriteLine($"接收到: {x}"),
    ex => Console.WriteLine($"流终止: {ex.Message}"),
    () => Console.WriteLine("流完成")
);
```

### 输出
```
接收到: 1
接收到: 2
接收到: 3
捕获异常: 发生错误！
接收到: 999
流完成
接收到: 42
流完成
```
