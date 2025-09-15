using System.Reactive.Concurrency;
using System.Reactive.Linq;

Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] Main thread");

// 不使用SubscribeOn,那么这里的Subscribe会在主线程执行,导致如果是Interval的话,那么程序就会卡这里不动了.
Observable
    .Range(1, 5)
    .Subscribe(tick => Console.WriteLine($"[T:{Environment.CurrentManagedThreadId}] Tick {tick}"));

Console.WriteLine("Range Done");

// 使用SubscribeOn到别地线程进行订阅,这样主线程继续执行.
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
