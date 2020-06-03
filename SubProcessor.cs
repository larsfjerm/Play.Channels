using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Play.Channels
{
    public class SubProcessor
    {
        private readonly int _processorId;
        private readonly Random _rnd;

        private long _readCount = 0;
        private long _writeCount = 0;

        Task _readerTask;
        CancellationTokenSource _ctSource;

        private readonly Channel<Guid> _apiDataChannel = Channel.CreateBounded<Guid>(new BoundedChannelOptions(1000)
        {
            SingleReader = true,
            SingleWriter = true,
        });

        public SubProcessor(int processorId)
        {
            _processorId = processorId;
            _rnd = new Random(processorId);
        }

        public void StartReader()
        {
            _ctSource = new CancellationTokenSource();

            _readerTask = Task.Factory.StartNew(
                async () =>
                {
                    var reader = _apiDataChannel.Reader;

                    try
                    {
                        await foreach (var message in _apiDataChannel.Reader.ReadAllAsync())
                        {
                            if (_readCount++ % 1000 == 0 && _rnd.Next(_processorId * 1000) < _processorId)
                                Console.WriteLine($"SubChannel {_processorId} \t Read {_readCount} \t");
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        //Console.WriteLine($"Exited channel {_processorId}");
                    }
                },
                _ctSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            );
        }

        public ValueTask Write(Guid message)
        {
            if (_writeCount++ % 1000 == 0 && _rnd.Next(_processorId * 1000) <= _processorId)
                Console.WriteLine($"SubChannel {_processorId} \t Written {_writeCount}");
            return _apiDataChannel.Writer.WriteAsync(message);
        }

        public Task Complete()
        {
            //Console.WriteLine($"Completing channel - Processor: {_processorId}");

            _ctSource.Cancel();

            if (!_apiDataChannel.Writer.TryComplete())
                Console.WriteLine($"Failed to complete the channel writer - Processor: {_processorId}");

            return _readerTask;
        }
    }
}
