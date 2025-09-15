using System.Reactive.Concurrency;
using System.Reactive.Linq;

static void Log(string tag, object? value) =>
    Console.WriteLine(
        $"[{DateTime.Now:HH:mm:ss.fff}] {tag, -22} | value={value, -4} | thread={Environment.CurrentManagedThreadId}"
    );

Console.WriteLine(new string('=', 60));
Console.WriteLine($"Main thread: {Environment.CurrentManagedThreadId}");
Console.WriteLine(new string('-', 60));
Console.WriteLine("Immediate SelectMany (ImmediateScheduler)");
Console.WriteLine(new string('-', 60));

Observable
    .Range(1, 5)
    .SelectMany(i => Observable.Range(i * 10, 5, ImmediateScheduler.Instance))
    .Subscribe(m => Log("Immediate.SelectMany", m));

Console.WriteLine(new string('-', 60));
Console.WriteLine("TaskPool Range (TaskPoolScheduler.Default)");
Console.WriteLine(new string('-', 60));

Observable.Range(1, 5, TaskPoolScheduler.Default).Subscribe(m => Log("TaskPool.Range", m));

Console.WriteLine(new string('-', 60));
Console.WriteLine("TaskPool SelectMany (TaskPoolScheduler.Default)");
Console.WriteLine(new string('-', 60));

Observable
    .Range(1, 5)
    .SelectMany(i => Observable.Range(i * 10, 5, TaskPoolScheduler.Default))
    .Subscribe(m => Log("TaskPool.SelectMany", m));

Console.WriteLine(new string('=', 60));
Console.WriteLine("Subscribe returned - press Enter to exit");
Console.WriteLine(new string('=', 60));
Console.ReadLine();
