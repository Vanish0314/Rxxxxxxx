using System.Reactive.Linq;

// 模拟一个可能抛出异常的 Observable
var source = Observable.Create<int>(observer =>
{
    observer.OnNext(1);
    observer.OnNext(2);
    observer.OnNext(3);
    observer.OnError(new Exception("发生错误！"));
    observer.OnNext(4); // 不会被执行
    return System.Reactive.Disposables.Disposable.Empty;
});

// 使用 Catch 捕获异常
var handled = source.Catch<int, Exception>(ex =>
{
    Console.WriteLine($"捕获异常: {ex.Message}");
    // 提供备用 Observable
    return Observable.Return(999);
});

handled.Subscribe(
    x => Console.WriteLine($"接收到: {x}"),
    ex => Console.WriteLine($"流终止: {ex.Message}"),
    () => Console.WriteLine("流完成")
);

var first = Observable.Throw<int>(new Exception("第一个流出错"));
var second = Observable.Return(42);

var result = first.Catch(second);

result.Subscribe(
    x => Console.WriteLine($"接收到: {x}"),
    ex => Console.WriteLine($"流终止: {ex.Message}"),
    () => Console.WriteLine("流完成")
);
