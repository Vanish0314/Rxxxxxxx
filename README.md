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
