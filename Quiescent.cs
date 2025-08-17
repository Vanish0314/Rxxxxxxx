using System.Reactive.Concurrency;
using System.Reactive.Linq;

static class RxExt
{
    public static IObservable<IList<T>> Quiescent<T>(
        this IObservable<T> src,
        TimeSpan minimumInactivityPeriod,
        IScheduler scheduler
    )
    {
        IObservable<int> onoffs =
            from _ in src
            from delta in Observable
                .Return(1, scheduler)
                .Concat(Observable.Return(-1, scheduler).Delay(minimumInactivityPeriod, scheduler))
            select delta;
        IObservable<int> outstanding = onoffs.Scan(0, (total, delta) => total + delta);
        IObservable<int> zeroCrossings = outstanding.Where(total => total == 0);
        return src.Buffer(zeroCrossings);
    }
}
