using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Play.Channels
{
    class Program
    {
        private static readonly CancellationTokenSource _ctSource = new CancellationTokenSource();

        private static long _readCount = 0;
        private static long _writeCount = 0;

        private static ConcurrentDictionary<int, SubProcessor> _subProcessorsDict = new ConcurrentDictionary<int, SubProcessor>();

        static async Task Main(string[] args)
        {
            var mainChannel = Channel.CreateBounded<Guid>(new BoundedChannelOptions(5_000)
            {
                SingleReader = true,
                SingleWriter = true
            });

            var readTask = Task.Factory.StartNew(async () =>
            {
                await foreach (var t in mainChannel.Reader.ReadAllAsync())
                {
                    foreach (var subChannel in _subProcessorsDict)
                    {
                        await subChannel.Value.Write(t);
                    }

                    if (_readCount++ % 1000 == 0)
                        Console.WriteLine($"MainChannel \t Read {_readCount} \t Subchannels written to: {_subProcessorsDict.Count}");
                }

                Console.WriteLine("MainChannel - Finished ReadAllAsync");
            }, _ctSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            Console.WriteLine("main Read task set.");

            for (var i = 0; i < 10_000; i++)
            {
                _subProcessorsDict[i] = new SubProcessor(i + 1);
                _subProcessorsDict[i].StartReader();
            }

            var writeTask = Task.Factory.StartNew(async () =>
            {
                while (!_ctSource.IsCancellationRequested)
                {
                    //await Task.Delay(10);

                    var guid = Guid.NewGuid();
                    await mainChannel.Writer.WriteAsync(guid, _ctSource.Token);

                    if (_writeCount++ % 1000 == 0)
                        Console.WriteLine($"MainChannel \t Written {_writeCount}");
                }
            }, _ctSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            Console.WriteLine("Main Write task set.");

            await Task.Delay(3_000);
            Console.WriteLine("Press Enter to cancel...");
            Console.ReadLine();

            Console.WriteLine("Cancelling");

            _ctSource.Cancel();

            await Task.WhenAll(_subProcessorsDict.Values.Select(x => x.Complete()));

            await writeTask;
            await readTask;

            Console.WriteLine("Channels completed");
        }
    }
}
