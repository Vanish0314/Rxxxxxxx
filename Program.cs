using System.Reactive.Concurrency;
using System.Reactive.Linq;

Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] Main thread");

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
        Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Tick {tick}")
    );

Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] {DateTime.Now}: Main thread exiting");