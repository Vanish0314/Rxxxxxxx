using System.Reactive.Disposables;
using System.Reactive.Linq;

static IObservable<int> WithCreate()
{
    return Observable.Create<int>(obs =>
    {
        Console.WriteLine("[CREATE] Doing some startup work...");
        for (int i = 0; i < 3; i++)
        {
            obs.OnNext(i);
        }
        obs.OnCompleted();
        return Disposable.Empty;
    });
}

Console.WriteLine("Calling factory method");
IObservable<int> s = WithCreate();

Console.WriteLine("First subscription");
s.Subscribe(Console.WriteLine);

Console.WriteLine("Second subscription");
s.Subscribe(Console.WriteLine);

Console.WriteLine("======================================");

static IObservable<int> WithDefer()
{
    return Observable.Defer(() =>
    {
        Console.WriteLine("[DEFER] Doing some startup work...");
        return Observable.Range(1, 3);
    });
}

Console.WriteLine("Calling factory method");
IObservable<int> s2 = WithDefer();

Console.WriteLine("First subscription");
s2.Subscribe(Console.WriteLine);

Console.WriteLine("Second subscription");
s2.Subscribe(Console.WriteLine);

Console.WriteLine("======================================");
