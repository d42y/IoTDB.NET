using System.Collections.Concurrent;


namespace IoTDBdotNET
{
    internal class TSBsonStorage : BaseDatabase
    {
        private readonly string _collectionName = "TimeSeriesData";
        private readonly ConcurrentQueue<(long EntityIndex, BsonValue Value, DateTime Timestamp)> _queue = new();

        private bool _queueProcessing = false;

        private readonly int _maxItemsPerFlush;

        public TSBsonStorage(string databasePath, string name, string? password) : base(databasePath, name, password, 10)
        {
            _maxItemsPerFlush = Helper.Limits.GetMaxProcessingItems();
        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {

                if (_queue.Count > 0 && !_queueProcessing)
                {

                    try
                    {
                        FlushQueue();
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                        lock (SyncRoot) { _queueProcessing = false; }
                    }
                }


            }

        }

        public void Add(long id, BsonValue value, DateTime timestamp = default)
        {
            if (timestamp == default) timestamp = DateTime.UtcNow;
            if (timestamp.Kind != DateTimeKind.Utc) timestamp = timestamp.ToUniversalTime();

            _queue.Enqueue((id, value, timestamp));
        }

        private void FlushQueue()
        {
            lock (SyncRoot)
            {
                _queueProcessing = true;
                try
                {

                    const int MaxItemsPerFlush = 5000; // Adjust this value as needed
                    int itemsProcessed = 0;

                    List<TSItem> items = new List<TSItem>();
                    while (_queue.TryDequeue(out var item) && itemsProcessed <= MaxItemsPerFlush)
                    {

                        items.Add(new() { EntityIndex = item.EntityIndex, Value = item.Value, Timestamp = item.Timestamp });
                        itemsProcessed++;
                    }
                    if (items.Count > 0)
                    {
                        
                            var collection = Database.GetCollection<TSItem>(_collectionName);
                            collection.Insert(items);
                        
                        //Database.Commit(); do not need to do LiteDB auto commit
                    }


                }
                catch (Exception ex) { OnExceptionOccurred(new(ex)); }
                _queueProcessing = false;
            }
        }

        public IEnumerable<TSItem> GetData(long id, DateTime from, DateTime to)
        {
            try
            {
                lock (SyncRoot)
                {
                    if (from.Kind != DateTimeKind.Utc)
                    {
                        from = from.ToUniversalTime();
                    }
                    if (to.Kind != DateTimeKind.Utc)
                    {
                        to = to.ToUniversalTime();
                    }
                    
                        var collection = Database.GetCollection<TSItem>(_collectionName);
                        var query = collection.Query()
                            .Where(x => x.Id == id && x.Timestamp >= from && x.Timestamp <= to)
                            .ToEnumerable();
                        return query;
                    
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return new List<TSItem>();
        }

        public IEnumerable<TSItem> GetData(List<long> ids, DateTime from, DateTime to)
        {
            try
            {
                lock (SyncRoot)
                {
                    if (from.Kind != DateTimeKind.Utc)
                    {
                        from = from.ToUniversalTime();
                    }
                    if (to.Kind != DateTimeKind.Utc)
                    {
                        to = to.ToUniversalTime();
                    }
                    
                        var collection = Database.GetCollection<TSItem>(_collectionName);
                        var query = collection.Query()
                            .Where(x => ids.Contains(x.Id) && x.Timestamp >= from && x.Timestamp <= to)
                            .ToEnumerable();
                        return query;
                    
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return new List<TSItem>();
        }

        protected override void InitializeDatabase()
        {
            //not necessary in this context
        }
    }

}
