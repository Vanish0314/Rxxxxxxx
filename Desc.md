
本示例展示 Rx.NET 中 SubscribeOn 与 Interval 调度器的关系：
- **Scenario A** 使用 ImmediateScheduler，Tick 在 EventLoopScheduler 线程执行
- **Scenario B** 使用默认线程池，Tick 在 ThreadPool 执行