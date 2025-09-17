using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

public class MainThreadScheduler : IScheduler
{
    private readonly int _mainThreadId;

    public MainThreadScheduler()
    {
        _mainThreadId = Environment.CurrentManagedThreadId;
    }

    public DateTimeOffset Now => DateTimeOffset.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        if (Environment.CurrentManagedThreadId != _mainThreadId)
            throw new InvalidOperationException("只能在主线程上调度");

        return action(this, state);
    }

    public IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        if (dueTime <= TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        Thread.Sleep(dueTime);
        return Schedule(state, action);
    }

    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<IScheduler, TState, IDisposable> action
    )
    {
        throw new NotImplementedException();
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        var subscription = Observable
            .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
            .SubscribeOn(new EventLoopScheduler())
            .Subscribe(Console.WriteLine);

        Console.WriteLine("Program Ended 0");

        var subscription0 = Observable
            .Interval(TimeSpan.FromSeconds(1), new MainThreadScheduler())
            .SubscribeOn(new EventLoopScheduler())
            .Subscribe(Console.WriteLine);

        Console.WriteLine("Program Ended 1");

        var subscription1 = Observable
            .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
            .Subscribe(Console.WriteLine);

        Console.WriteLine("Program REALLY Ended");
    }
}
