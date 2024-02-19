using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Text;
using System.Threading;
using IoTDBdotNET;

namespace IoTDBdotNET
{
    internal class TimeSeriesDatabase : BaseDatabase, IDisposable, ITimeSeriesDatabase
    {
        private readonly string _collectionName = "Entities";
        private ConcurrentDictionary<long, TSNumericStorage> _numericStorage { get; } = new();
        private ConcurrentDictionary<long, TSBsonStorage> _bsonStorage { get; } = new();
        private ConcurrentDictionary<string, Entity> _entities { get; } = new();
        private ConcurrentQueue<string> _updateEntityQueue = new ConcurrentQueue<string>();
        private bool _processingQueue = false;
        private int _roundRobinCounter = 0; // Added for round-robin storage selection

        public TimeSeriesDatabase(string dbPath) : base(dbPath, "index")
        {
            try
            {
                InitializeDatabase();
                InitializeTimeSeriesStorages(dbPath);
            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
        }

        #region Init

        protected override void InitializeDatabase()
        {
            try
            {
                var entities = Database.GetCollection<Entity>(_collectionName);
                entities.EnsureIndex(x => x.Guid, true);

            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
        }

        private void InitializeTimeSeriesStorages(string dbPathName)
        {
            for (int i = 1; i <= NumThreads; i++)
            {
                _numericStorage[i] = new TSNumericStorage(i.ToString(), Path.Combine(dbPathName, $"TimeSeries"), true);
                _bsonStorage[i] = new TSBsonStorage(Path.Combine(dbPathName, "TimeSeries"), $"{i}_TimeSeries.db");
            }
        }
        #endregion Init

        #region background task
        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            if (!_updateEntityQueue.IsEmpty && !_processingQueue)
            {
                lock (SyncRoot)
                {
                    _processingQueue = true;
                    try
                    {
                        int count = 0;
                        Dictionary<string, Entity> entityList = new();
                        List<string> guids = new List<string>();
                        while (_updateEntityQueue.TryDequeue(out var guid) && count++ <= 100)
                        {
                            if (!entityList.ContainsKey(guid))
                            {
                                entityList.Add(guid, new());
                            }
                            if (_entities.TryGetValue(guid, out var entity))
                            {
                                entityList[guid] = entity;
                            }
                        }
                        if (entityList.Count > 0)
                        {
                            var entitites = Database.GetCollection<Entity>(_collectionName);
                            entitites.Update(entityList.Select(x => x.Value).ToList());
                        }

                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                    }
                    _processingQueue = false;
                }
            }
        }
        #endregion background task

        #region Set

        public void Set(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
            SetAsync(guid, value, timestamp, timeSeries).Wait();
        }

        public async Task SetAsync(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
            Entity? entity = null;
            if (_entities.ContainsKey(guid))
            {
                entity = _entities[guid];
                entity.Value = value;
                entity.Timestamp = timestamp;
                _updateEntityQueue.Enqueue(guid);
            }
            else
            {
                entity = AddUpdateEntity(guid, value, timestamp);
                if (entity == null) throw new Exception($"Unable to create entity for GUID: [{guid}]");
                _entities[guid] = entity;
            }

            if (timestamp.Kind != DateTimeKind.Utc) timestamp = DateTime.UtcNow;

            var storageKey = GetRoundRobinStorageKey();
            if (value.IsNumber && timeSeries)
            {
                var selectedTimeSeriesStorage = _numericStorage[storageKey];
                selectedTimeSeriesStorage.Add(entity.Id, value.AsDouble, timestamp);
            }
            else
            {
                var selectedTimeSeriesStorage = _bsonStorage[storageKey];
                selectedTimeSeriesStorage.Add(entity.Id, value, timestamp);
            }
        }

        #endregion Set

        #region Get
        public async Task<(BsonValue Value, DateTime Timestamp)> GetAsync(string guid)
        {
            if (_entities.ContainsKey(guid))
            {
                if (_entities.TryGetValue(guid, out var e))
                {
                    return (e.Value, e.Timestamp);
                }
            }
            var entity = GetEntity(guid);
            if (entity == null) throw new KeyNotFoundException($"Guid not found: {guid}");
            _entities[guid] = entity;
            return (entity.Value, entity.Timestamp);
        }

        public async Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to)
        {
            Dictionary<long, string> entityIdToGuidMap = new Dictionary<long, string>();
            List<long> entityIds = new List<long>();

            // Build entityId to Guid map and list of entityIds
            foreach (var guid in guids)
            {
                if (_entities.TryGetValue(guid, out var entity))
                {
                    entityIds.Add(entity.Id);
                    entityIdToGuidMap[entity.Id] = guid;
                }
                else
                {
                    throw new KeyNotFoundException($"Guid not found: {guid}");
                }
            }

            // Fetch data from numeric storages
            var fetchTasks = _numericStorage.Values.Select(storage => Task.Run(() => storage.GetData(entityIds, from, to))).ToArray();
            var results = await Task.WhenAll(fetchTasks);

            // Fetch data from BSON storages
            var fetchBsonTasks = _bsonStorage.Values.Select(storage => Task.Run(() => storage.GetData(entityIds, from, to))).ToArray();
            var results2 = await Task.WhenAll(fetchBsonTasks);

            // Combine results from both storage types
            var combinedResults = results.Concat(results2).SelectMany(result => result);

            // Convert combined results to TimeSeriesItem and group by Guid
            var groupedResultsWithGuid = combinedResults
                .Select(item => new TimeSeriesItem
                {
                    Guid = entityIdToGuidMap[item.Id],
                    EntityIndex = item.EntityIndex,
                    Value = item.Value,
                    Timestamp = item.Timestamp
                })
                .GroupBy(item => item.Guid)
                .ToDictionary(group => group.Key, group => group.ToList());

            return groupedResultsWithGuid;
        }



        public async Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to, int interval, IntervalType intervalType)
        {
            var rawData = await GetAsync(guids, from, to);

            if (rawData == null || !rawData.Any()) return new Dictionary<string, List<TimeSeriesItem>>();

            var interpolatedData = new Dictionary<string, List<TimeSeriesItem>>();

            foreach (var guid in rawData.Keys)
            {
                var groupData = rawData[guid].OrderBy(d => d.Timestamp).ToList();
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
                            groupInterpolatedData.Add(new TimeSeriesItem
                            {
                                Guid = guid,
                                EntityIndex = currentData.EntityIndex,
                                Value = interpolatedValue,
                                Timestamp = nextIntervalPoint
                            });
                        }

                        nextIntervalPoint = GetNextIntervalPoint(nextIntervalPoint, interval, intervalType);
                    }
                }

                if (groupInterpolatedData.Any())
                {
                    interpolatedData[guid] = groupInterpolatedData;
                }
            }

            return interpolatedData;
        }


        #endregion Get

        #region Entities
        private Entity? AddUpdateEntity(string guid, BsonValue value, DateTime timestamp)
        {
            try
            {
                var entities = Database.GetCollection<Entity>(_collectionName);
                var entity = entities.FindOne(e => e.Guid == guid);
                if (entity == null)
                {
                    entity = new Entity { Guid = guid, Value = value, Timestamp = timestamp };
                    var id = entities.Insert(entity);
                    return entity;
                }
                else
                {
                    entity.Value = value;
                    entity.Timestamp = timestamp;
                    entities.Update(entity);
                }
                //return existing entity
                return entity;
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return null;
        }

        private Entity? GetEntity(long id)
        {
            try
            {

                var entities = Database.GetCollection<Entity>(_collectionName);

                // Find the entity by its Id
                var entity = entities.FindOne(e => e.Id == id);
                return entity;
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return null; // If no entity is found, return null

        }

        private Entity? GetEntity(string guid)
        {
            try
            {

                var entities = Database.GetCollection<Entity>(_collectionName);

                // Find the entity by its Id
                var entity = entities.FindOne(e => e.Guid == guid);
                return entity;
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return null; // If no entity is found, return null

        }

        // Method to asynchronously update an entity's GUID
        public void UpdateEntityGuid(long id, string newGuid)
        {
            try
            {
                var entities = Database.GetCollection<Entity>(_collectionName);
                var entity = entities.FindOne(e => e.Id == id);
                if (entity != null)
                {
                    entity.Guid = newGuid;
                    entities.Update(entity);
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        private void DeleteEntity(long id)
        {
            try
            {
                var entities = Database.GetCollection<Entity>(_collectionName);

                // First, find the entity to get its GUID
                var entity = entities.FindById(id);
                if (entity != null)
                {
                    // Delete the entity by ID
                    entities.Delete(id);
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        public void DeleteEntity(string guid)
        {
            try
            {
                var entities = Database.GetCollection<Entity>(_collectionName);
                // Delete the entity by GUID
                var entityDeleted = entities.DeleteMany(e => e.Guid == guid) > 0;
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }
        #endregion Entitites

        #region Util

        private int GetRoundRobinStorageKey()
        {
            // Safely increment and get the value for round-robin approach
            int currentCounter = Interlocked.Increment(ref _roundRobinCounter);

            // Ensure the counter wraps around correctly
            if (currentCounter >= int.MaxValue) Interlocked.Exchange(ref _roundRobinCounter, 0);

            // Use the counter to select the storage, ensuring it wraps around the number of threads
            return currentCounter % NumThreads + 1;

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
        #endregion Util


        // Helper classes to represent documents in LiteDB
        private class Entity
        {
            public long Id { get; set; }
            public string Guid { get; set; }
            public BsonValue Value { get; set; } = BsonValue.Null;
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        }

    }

}