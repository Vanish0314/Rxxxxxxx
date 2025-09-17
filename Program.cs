using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

var subscription = Observable
    .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
    .SubscribeOn(new EventLoopScheduler())
    .Subscribe(Console.WriteLine);

Console.WriteLine("Program Ended");

var subscription0 = Observable
    .Interval(TimeSpan.FromSeconds(1), ImmediateScheduler.Instance)
    .Subscribe(Console.WriteLine);

Console.WriteLine("Program REALLY Ended");