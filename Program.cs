using System.Reactive.Linq;

IObservable<long> ticks = Observable.Timer(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(1));

ticks.Subscribe(tick => Console.WriteLine($"Tick {tick}"));

Console.ReadLine();
