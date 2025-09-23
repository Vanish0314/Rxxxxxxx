using System.Reactive;
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
