using System.Reactive.Concurrency;
using System.Reactive.Linq;

Console.WriteLine("Quiescent 示例 - 检测事件流的静默期");
Console.WriteLine("=====================================");

Int32 eventCount = 0;

var random = new Random();
var eventStream = Observable.Create<int>(observer =>
{
    return Observable
        .Interval(TimeSpan.FromMilliseconds(500))
        .Subscribe(_ =>
        {
            if (random.Next(100) < 30) // 30% 的概率生成事件
            {
                var delay = random.Next(0, 5000); // 0-5秒的随机延迟

                Observable
                    .Timer(TimeSpan.FromMilliseconds(delay))
                    .Subscribe(__ => observer.OnNext(eventCount++));
            }
        });
});

var quiescentStream = eventStream.Quiescent(TimeSpan.FromSeconds(1), Scheduler.Default);

eventStream.Subscribe(eventId =>
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] 事件: {eventId}")
);

quiescentStream.Subscribe(events =>
{
    if (events.Count > 0)
    {
        Console.WriteLine(
            $"\n[{DateTime.Now:HH:mm:ss.fff}] 静默期结束！收集到 {events.Count} 个事件:"
        );
        foreach (var evt in events)
        {
            Console.WriteLine($"  - 事件 {evt}");
        }
        Console.WriteLine();
    }
    else
    {
        Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] 静默期结束（无事件）");
    }
});

Console.WriteLine("程序运行中... 按任意键退出");
Console.WriteLine("静默期设置为 2 秒");
Console.WriteLine("事件以随机间隔（0-5秒）生成");
Console.WriteLine("=====================================\n");

while (true) { }
