using System.Reactive.Concurrency;
using System.Reactive.Linq;

IObservable<long> source = Observable.Interval(TimeSpan.FromSeconds(1)).Take(5);

Console.WriteLine("ForEachAsync start...");
await source.ForEachAsync(i => Console.WriteLine($"received {i} @ {DateTime.Now}"));
Console.WriteLine($"ForEachAsync finished @ {DateTime.Now}");

Console.WriteLine("Subscribe start...");
source.Subscribe(i => Console.WriteLine($"Subscribe received {i} @ {DateTime.Now}"));
Console.WriteLine($"Subscribe finished @ {DateTime.Now}");

Console.WriteLine("Subscribe ImmediateScheduler start...");
IObservable<long> source1 = Observable
    .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
    .Take(5);
source1.Subscribe(i =>
    Console.WriteLine($"ImmediateScheduler subscriber received {i} @ {DateTime.Now}")
);
Console.WriteLine($"Subscribe ImmediateScheduler finished @ {DateTime.Now}");
