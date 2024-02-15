using LiteDB;
using System.Collections.Concurrent;

namespace IoTDB.NET
{
    public class IoTDB
    {
        // Define the event based on the delegate
        event EventHandler<ExceptionEventArgs> ExceptionOccurred;

        private readonly int _numThreads;
        public IEntityDatabase Entities { get; }
        private ConcurrentDictionary<long, TimeSeriesStorage> TimeSeriesStorages { get; } = new ();
        private ConcurrentDictionary<long, LiteDBTimeSeriesStorage> LiteDBTimeSeriesStorages { get; } = new();

        private int _roundRobinCounter = 0; // Added for round-robin storage selection

        private ConcurrentDictionary<long, string> _entities = new ConcurrentDictionary<long, string>();
        private ConcurrentQueue<Action> _operationsQueue = new ConcurrentQueue<Action>();
        private Task _workerTask;
        private bool _isWorkerRunning = true;

        public IoTDB(string dbName, string dbPath, bool createPathIfNotExist = false)
        {
            int logicalProcessorCount = Environment.ProcessorCount;
            if (logicalProcessorCount - 1 > 1) _numThreads = logicalProcessorCount - 1;
            else _numThreads = 1;
            // Directory checks and creation
            InitializeDirectories(dbPath, createPathIfNotExist);

            var dbPathName = Path.Combine(dbPath, dbName);
            InitializeDbPathName(dbPathName);

            // Initialize entity database
            Entities = new EntityDatabase(Path.Combine(dbPathName, "index.db"));
            Entities.ExceptionOccurred += OnExceptionOccurred;

            // Initialize multiple TimeSeriesStorages
            InitializeTimeSeriesStorages(dbPathName);

            // Start worker task
            _workerTask = Task.Run(() => ProcessQueue());
        }


        private void InitializeDirectories(string dbPath, bool createPathIfNotExist)
        {
            if (!Directory.Exists(dbPath) && createPathIfNotExist)
            {
                Directory.CreateDirectory(dbPath);
            }
        }

        private void InitializeDbPathName(string dbPathName)
        {
            if (!Directory.Exists(dbPathName))
            {
                Directory.CreateDirectory(dbPathName);
            }
        }

        private void InitializeTimeSeriesStorages(string dbPathName)
        {
            for (int i = 1; i <= _numThreads; i++)
            {
                TimeSeriesStorages[i] = new TimeSeriesStorage(i.ToString(), Path.Combine(dbPathName, $"TimeSeries"), true);
                LiteDBTimeSeriesStorages[i] = new LiteDBTimeSeriesStorage(Path.Combine(dbPathName, $"TimeSeries_{i}.db"));
            }
        }

        private void ProcessQueue()
        {
            // Process operations queue
            while (_isWorkerRunning || !_operationsQueue.IsEmpty)
            {
                if (_operationsQueue.TryDequeue(out var operation))
                {
                    operation.Invoke();
                }
                else
                {
                    Task.Delay(100).Wait();
                }
            }
        }

        public async Task Set(long entityId, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
            await ValidateEntityExistsAsync(entityId);
            _operationsQueue.Enqueue(() => SetOperation(entityId, value, timestamp, timeSeries));

        }

        private void SetOperation(long entityId, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
            if (timestamp == default) timestamp = DateTime.UtcNow;
            Entities.AddOrUpdateProperties(_entities[entityId], (PropertyName.Value, value));
            // Use round-robin to determine storage key
            var storageKey = GetRoundRobinStorageKey();
            if (value.IsNumber && timeSeries)
            {
                var selectedTimeSeriesStorage = TimeSeriesStorages[storageKey];
                selectedTimeSeriesStorage.Add(entityId, value.AsDouble, timestamp);
            }
            else
            {
                var selectedTimeSeriesStorage = LiteDBTimeSeriesStorages[storageKey];
                selectedTimeSeriesStorage.Add(entityId, value, timestamp);
            }
        }
        private int GetRoundRobinStorageKey()
        {
            // Safely increment and get the value for round-robin approach
            int currentCounter = Interlocked.Increment(ref _roundRobinCounter);

            // Ensure the counter wraps around correctly
            if (currentCounter >= int.MaxValue) Interlocked.Exchange(ref _roundRobinCounter, 0);

            // Use the counter to select the storage, ensuring it wraps around the number of threads
            return currentCounter % _numThreads + 1;
        }

        private async Task ValidateEntityExistsAsync(long entityId)
        {
            if (!_entities.ContainsKey(entityId) && !await IsEntityAsync(new() { entityId}))
            {
                throw new KeyNotFoundException($"Entity Id not found: {entityId}");
            }
        }

        public async Task<(double Value, DateTime Timestamp)?> GetAsync(long entityId)
        {
            await IsEntityAsync(new() { entityId});
            if (!_entities.ContainsKey(entityId)) { throw new KeyNotFoundException($"GUID not found for Entity Id: {entityId}"); }

            var values = Entities.GetProperties(_entities[entityId], PropertyName.Value, PropertyName.Timestamp);
            if (values.ContainsKey(PropertyName.Value) && values.ContainsKey(PropertyName.Timestamp))
            {
                if (double.TryParse(values[PropertyName.Value].ToString(), out double value))
                {
                    if (DateTime.TryParse(values[PropertyName.Timestamp].ToString(), out DateTime timestamp))
                    return (value, timestamp);
                }
            }

            return null;
        }

        public async Task<Dictionary<long, List<TimeSeriesItem>>> GetAsync(List<long> entityIds, DateTime from, DateTime to)
        {
            await IsEntityAsync(entityIds);

            // Use tasks to fetch data from all storages concurrently
            var fetchTasks = TimeSeriesStorages.Values.Select(storage => Task.Run(() => storage.GetData(entityIds, from, to))).ToArray();

            // Wait for all tasks to complete
            var results = await Task.WhenAll(fetchTasks);

            // Combine results from all storages
            var combinedResults = results.SelectMany(result => result);

            // Group combined results by Id and convert to dictionary
            var groupedResults = combinedResults
                .GroupBy(item => item.Id)
                .ToDictionary(group => group.Key, group => group.ToList());

            return groupedResults;
        }


        public async Task<Dictionary<long, List<TimeSeriesItem>>> GetAsync(List<long> entityIds, DateTime from, DateTime to, int interval, IntervalType intervalType)
        {
            Dictionary<long, List<TimeSeriesItem>>? rawData;
            try
            {
                rawData = await GetAsync(entityIds, from, to);
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to get time series data.", ex);
            }

            if (rawData == null || !rawData.Any()) return new Dictionary<long, List<TimeSeriesItem>>();

            var interpolatedData = new Dictionary<long, List<TimeSeriesItem>>();

            foreach (var groupId in rawData.Keys)
            {
                var groupData = rawData[groupId].OrderBy(d => d.Timestamp).ToList();
                var groupInterpolatedData = new List<TimeSeriesItem>();
                DateTime nextIntervalPoint = GetNextIntervalPoint(from, interval, intervalType);

                for (int i = 0; i < groupData.Count - 1; i++)
                {
                    var currentData = groupData[i];
                    var nextData = groupData[i + 1];

                    while (nextIntervalPoint < nextData.Timestamp)
                    {
                        if (nextIntervalPoint > currentData.Timestamp)
                        {
                            var interpolatedValue = LinearInterpolate(currentData, nextData, nextIntervalPoint);
                            groupInterpolatedData.Add(new TimeSeriesItem { Timestamp = nextIntervalPoint, Id = groupId, Value = interpolatedValue });
                        }

                        nextIntervalPoint = GetNextIntervalPoint(nextIntervalPoint, interval, intervalType);
                    }
                }

                if (groupInterpolatedData.Any())
                {
                    interpolatedData.Add(groupId, groupInterpolatedData);
                }
            }

            return interpolatedData;
        }

        private async Task<bool> IsEntityAsync(List<long> entityIds)
        {
            foreach (var id in entityIds)
            {
                if (!_entities.ContainsKey(id))
                {
                    var e = Entities.GetEntity(id);
                    if (e == null) throw new KeyNotFoundException($"Entity Id not found: {id}"); 

                    _entities.AddOrUpdate(e.Value.Id, e.Value.Guid, (key, oldValue) => e.Value.Guid);
                }
            }
            return true;
        }

        private DateTime GetNextIntervalPoint(DateTime start, int interval, IntervalType intervalType)
        {
            switch (intervalType)
            {
                case IntervalType.Seconds:
                    return start.AddSeconds(interval);
                case IntervalType.Minutes:
                    return start.AddMinutes(interval);
                case IntervalType.Hours:
                    return start.AddHours(interval);
                case IntervalType.Days:
                    return start.AddDays(interval);
                case IntervalType.Weeks:
                    return start.AddDays(7 * interval); // 1 week = 7 days
                case IntervalType.Months:
                    return start.AddMonths(interval);
                case IntervalType.Years:
                    return start.AddYears(interval);
                default:
                    throw new ArgumentOutOfRangeException(nameof(intervalType), $"Not expected interval type: {intervalType}");
            }
        }

        private double LinearInterpolate(TimeSeriesItem start, TimeSeriesItem end, DateTime target)
        {
            double fraction = (target - start.Timestamp).TotalSeconds / (end.Timestamp - start.Timestamp).TotalSeconds;
            return start.Value + fraction * (end.Value - start.Value);
        }

        protected virtual void OnExceptionOccurred(object? sender, ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(sender??this, e);
        }
    }
}
