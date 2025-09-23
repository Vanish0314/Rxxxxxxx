using System.Reactive;
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
