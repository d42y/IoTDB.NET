using LiteDB;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDB.NET
{
    internal class LiteDBTimeSeriesStorage : IDisposable
    {
        public event EventHandler<ExceptionEventArgs> ExceptionOccurred;

        private readonly string _databasePath;
        private readonly LiteDatabase _db;
        private readonly string _collectionName = "TimeSeriesData";
        private readonly ConcurrentQueue<(long EntityIndex, BsonValue Value, DateTime Timestamp)> _queue = new();
        private CancellationTokenSource _cancellationTokenSource = new();
        private Task _writeTask;
        private readonly object _syncRoot = new object();
        private bool _queueProcessing = false;

        public LiteDBTimeSeriesStorage(string databasePath)
        {
            _databasePath = databasePath;
            _db = new LiteDatabase(_databasePath);
            StartBackgroundTask();
        }

        private void StartBackgroundTask()
        {
            _writeTask = Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        if (_queue.Count > 0 && !_queueProcessing)
                        {
                            FlushQueue();
                        }
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                        lock(_syncRoot) { _queueProcessing = false; }
                        
                        
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(10), _cancellationTokenSource.Token);
                }
            }, _cancellationTokenSource.Token);
        }

        public void Add(long id, BsonValue value, DateTime timestamp = default)
        {
            if (timestamp == default) timestamp = DateTime.UtcNow;
            if (timestamp.Kind != DateTimeKind.Utc) timestamp = timestamp.ToUniversalTime();

            _queue.Enqueue((id, value, timestamp));
        }

        private void FlushQueue()
        {
            lock (_syncRoot)
            {
                _queueProcessing = true;
                try
                {

                    const int MaxItemsPerFlush = 5000; // Adjust this value as needed
                    int itemsProcessed = 0;

                    List<TimeSeriesItem> items = new List<TimeSeriesItem>();
                    while (_queue.TryDequeue(out var item) && itemsProcessed <= MaxItemsPerFlush)
                    {

                        items.Add(new() { EntityIndex = item.EntityIndex, Value = item.Value, Timestamp = item.Timestamp });
                        itemsProcessed++;
                    }
                    if (items.Count > 0)
                    {
                        var collection = _db.GetCollection<TimeSeriesItem>("Timeseries");
                        collection.Insert(items);
                        //_db.Commit(); do not need to do LiteDB auto commit
                    }


                }
                catch (Exception ex) { OnExceptionOccurred(new(ex)); }
                _queueProcessing = false;
            }
        }

        public IEnumerable<TimeSeriesItem> GetData(int id, DateTime from, DateTime to)
        {
            try
            {
                lock (_syncRoot)
                {
                    if (from.Kind != DateTimeKind.Utc)
                    {
                        from = from.ToUniversalTime();
                    }
                    if (to.Kind != DateTimeKind.Utc)
                    {
                        to = to.ToUniversalTime();
                    }
                    var collection = _db.GetCollection<TimeSeriesItem>(_collectionName);
                    var query = collection.Query()
                        .Where(x => x.Id == id && x.Timestamp >= from && x.Timestamp <= to)
                        .ToEnumerable();
                    return query;
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return new List<TimeSeriesItem>();
        }

        protected virtual void OnExceptionOccurred(ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(this, e);
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _writeTask?.Wait();
            _db.Dispose();
        }

        
    }

}
