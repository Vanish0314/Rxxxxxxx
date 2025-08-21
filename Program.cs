using System.Reactive.Linq;
using System.Text;

namespace Rxxxxxxx
{
    // 将文件系统变更表示为 Rx 的可观察序列。
    // 注意：这是一个过度简化的示例，仅用于演示。
    //       它不能高效地处理多个订阅者，也没有使用 IScheduler，
    //       并且在第一个错误发生时就会立即停止。
    public class RxFsEvents(string folder) : IObservable<FileSystemEventArgs>
    {
        private readonly string folder = folder;

        public IDisposable Subscribe(IObserver<FileSystemEventArgs> observer)
        {
            // 如果有多个订阅者，这会很低效。
            FileSystemWatcher watcher = new(folder);

            object sync = new();
            bool onErrorAlreadyCalled = false;

            void SendToObserver(object _, FileSystemEventArgs e)
            {
                lock (sync)
                {
                    if (!onErrorAlreadyCalled)
                    {
                        observer.OnNext(e);
                    }
                }
            }

            watcher.Created += SendToObserver;
            watcher.Changed += SendToObserver;
            watcher.Renamed += SendToObserver;
            watcher.Deleted += SendToObserver;

            watcher.Error += (_, e) =>
            {
                lock (sync)
                {
                    if (!onErrorAlreadyCalled)
                    {
                        observer.OnError(e.GetException());
                        onErrorAlreadyCalled = true;
                        watcher.Dispose();
                    }
                }
            };

            watcher.EnableRaisingEvents = true;

            return watcher;
        }
    }

    public class RxFsEventsMultiSubscriber : IObservable<FileSystemEventArgs>
    {
        private readonly Lock sync = new();
        private readonly List<Subscription> subscribers = [];
        private readonly FileSystemWatcher watcher;

        public RxFsEventsMultiSubscriber(string folder)
        {
            watcher = new FileSystemWatcher(folder);

            watcher.Created += SendEventToObservers;
            watcher.Changed += SendEventToObservers;
            watcher.Renamed += SendEventToObservers;
            watcher.Deleted += SendEventToObservers;

            watcher.Error += SendErrorToObservers;
        }

        public IDisposable Subscribe(IObserver<FileSystemEventArgs> observer)
        {
            Subscription sub = new(this, observer);
            lock (sync)
            {
                subscribers.Add(sub);
                if (subscribers.Count == 1)
                {
                    watcher.EnableRaisingEvents = true;
                }
            }
            return sub;
        }

        private void Unsubscribe(Subscription sub)
        {
            lock (sync)
            {
                subscribers.Remove(sub);
                if (subscribers.Count == 0)
                {
                    watcher.EnableRaisingEvents = false;
                }
            }
        }

        void SendEventToObservers(object _, FileSystemEventArgs e)
        {
            lock (sync)
            {
                foreach (var subscription in subscribers)
                {
                    subscription.Observer.OnNext(e);
                }
            }
        }

        void SendErrorToObservers(object _, ErrorEventArgs e)
        {
            Exception x = e.GetException();
            lock (sync)
            {
                foreach (var subscription in subscribers)
                {
                    subscription.Observer.OnError(x);
                }
                subscribers.Clear();
            }
        }

        private class Subscription : IDisposable
        {
            private RxFsEventsMultiSubscriber? parent;

            public Subscription(
                RxFsEventsMultiSubscriber rxFsEventsMultiSubscriber,
                IObserver<FileSystemEventArgs> observer
            )
            {
                parent = rxFsEventsMultiSubscriber;
                Observer = observer;
            }

            public IObserver<FileSystemEventArgs> Observer { get; }

            public void Dispose()
            {
                parent?.Unsubscribe(this);
                parent = null;
            }
        }
    }

    internal class FileSystemObserver : IObserver<FileSystemEventArgs>
    {
        public void OnNext(FileSystemEventArgs value)
        {
            Console.WriteLine($"文件系统事件: {value.ChangeType} - {value.FullPath}");
        }

        public void OnError(Exception error)
        {
            Console.WriteLine($"错误: {error.Message}");
        }

        public void OnCompleted()
        {
            Console.WriteLine("文件系统监控完成");
        }
    }

    internal class Program
    {
        static IObservable<FileSystemEventArgs> ObserveFileSystem(string folder)
        {
            return Observable
                .Defer(() =>
                {
                    FileSystemWatcher fsw = new(folder) { EnableRaisingEvents = true };
                    return Observable.Return(fsw);
                })
                .SelectMany(fsw =>
                    Observable
                        .Merge(
                            [
                                Observable.FromEventPattern<
                                    FileSystemEventHandler,
                                    FileSystemEventArgs
                                >(h => fsw.Created += h, h => fsw.Created -= h),
                                Observable.FromEventPattern<
                                    FileSystemEventHandler,
                                    FileSystemEventArgs
                                >(h => fsw.Changed += h, h => fsw.Changed -= h),
                                Observable.FromEventPattern<
                                    RenamedEventHandler,
                                    FileSystemEventArgs
                                >(h => fsw.Renamed += h, h => fsw.Renamed -= h),
                                Observable.FromEventPattern<
                                    FileSystemEventHandler,
                                    FileSystemEventArgs
                                >(h => fsw.Deleted += h, h => fsw.Deleted -= h),
                            ]
                        )
                        .Select(ep => ep.EventArgs)
                        .Finally(() => fsw.Dispose())
                )
                .Publish()
                .RefCount();
        }

        static void Main()
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.InputEncoding = Encoding.UTF8;
            Console.WriteLine("文件系统监控示例");
            Console.WriteLine("==================");

            // 示例1: 使用简单的RxFsEvents
            Console.WriteLine("\n示例1: 使用简单的RxFsEvents");
            var simpleWatcher = new RxFsEvents("C:/");
            var subscription1 = simpleWatcher.Subscribe(new FileSystemObserver());

            // 示例2: 使用多订阅者版本
            Console.WriteLine("\n示例2: 使用多订阅者版本");
            var multiWatcher = new RxFsEventsMultiSubscriber("C:/");
            var subscription2 = multiWatcher.Subscribe(new FileSystemObserver());
            var subscription3 = multiWatcher.Subscribe(new FileSystemObserver());

            // 示例3: 使用Observable.FromEventPattern
            Console.WriteLine("\n示例3: 使用Observable.FromEventPattern");
            FileSystemWatcher watcher = new("C:/");
            IObservable<FileSystemEventArgs> changes = Observable
                .FromEventPattern<FileSystemEventHandler, FileSystemEventArgs>(
                    h => watcher.Changed += h,
                    h => watcher.Changed -= h
                )
                .Select(ep => ep.EventArgs);

            var subscription4 = changes.Subscribe(new FileSystemObserver());
            watcher.EnableRaisingEvents = true;

            // 示例4: 使用ObserveFileSystem方法
            Console.WriteLine("\n示例4: 使用ObserveFileSystem方法");
            var fileSystemEvents = ObserveFileSystem("C:/");
            var subscription5 = fileSystemEvents.Subscribe(new FileSystemObserver());

            Console.WriteLine("\n按任意键退出...");
            while (true) { }

            // 清理订阅
            subscription1?.Dispose();
            subscription2?.Dispose();
            subscription3?.Dispose();
            subscription4?.Dispose();
            subscription5?.Dispose();
            watcher?.Dispose();
        }
    }
}
