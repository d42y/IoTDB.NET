using System;
using System.Collections.Concurrent;

namespace IoTDBdotNET
{
    internal class TimeSeriesDatabase : BaseDatabase, IDisposable, ITimeSeriesDatabase
    {
        private readonly string _collectionName = "Entities";
        private ConcurrentDictionary<long, TSNumericStorage> _numericStorage { get; } = new();
        private ConcurrentDictionary<long, TSBsonStorage> _bsonStorage { get; } = new();
        private ConcurrentDictionary<string, Entity> _entities { get; } = new();
        private ConcurrentQueue<(string guid, BsonValue value, DateTime timestamp, bool timeseries)> _updateEntityQueue = new ();
        private bool _processingQueue = false;
        private int _roundRobinCounter = 0; // Added for round-robin storage selection

        private readonly int _maxItemsPerFlush;

        public TimeSeriesDatabase(string dbPath) : base(dbPath, "index")
        {
            try
            {
                _maxItemsPerFlush = Helper.Limits.GetMaxProcessingItems();
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
                _numericStorage[i] = new TSNumericStorage(i.ToString(), DbPath, true);
                _bsonStorage[i] = new TSBsonStorage(DbPath, $"{i}_BsonSeries");
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
                        
                            var entitites = Database.GetCollection<Entity>(_collectionName);
                            int count = 0;
                            Dictionary<string, Entity> entityList = new();
                            List<string> guids = new List<string>();
                            while (_updateEntityQueue.TryDequeue(out var updateEntity) && count++ <= _maxItemsPerFlush)
                            {
                                try
                                {
                                    Entity? entity = null;
                                    if (_entities.ContainsKey(updateEntity.guid))
                                    {
                                        entity = _entities[updateEntity.guid];
                                        entity.Value = updateEntity.value;
                                        entity.Timestamp = updateEntity.timestamp;
                                    }

                                    if (entity == null || entity.Id == 0)
                                    {
                                        entity = new()
                                        {
                                            Guid = updateEntity.guid,
                                            Value = updateEntity.value,
                                            Timestamp = updateEntity.timestamp
                                        };
                                        entity = AddUpdateEntity(updateEntity.guid, updateEntity.value, updateEntity.timestamp, entitites);
                                        if (entity == null)
                                        {
                                            entity = GetEntity(updateEntity.guid);
                                            if (entity == null && _entities.ContainsKey(updateEntity.guid)) entity = _entities[updateEntity.guid];
                                            if (entity == null) break;
                                        }
                                    }
                                    else
                                    {
                                        if (entityList.ContainsKey(entity.Guid))
                                        {
                                            entityList[entity.Guid] = entity;
                                        }
                                        else
                                        {
                                            entityList.Add(entity.Guid, entity);
                                        }
                                    }

                                    if (updateEntity.timeseries)
                                    {
                                        var storageKey = GetRoundRobinStorageKey();
                                        if (entity.Value.IsNumber)
                                        {
                                            var selectedTimeSeriesStorage = _numericStorage[storageKey];
                                            selectedTimeSeriesStorage.Add(entity.Id, entity.Value.AsDouble, entity.Timestamp);
                                        }
                                        else
                                        {
                                            var selectedTimeSeriesStorage = _bsonStorage[storageKey];
                                            selectedTimeSeriesStorage.Add(entity.Id, entity.Value, entity.Timestamp);
                                        }
                                    }
                                }
                                catch { _updateEntityQueue.Enqueue(updateEntity); }
                            }
                            if (entityList.Count > 0)
                            {

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

        #region Insert

        public void Insert(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
            if (timestamp == default) timestamp = DateTime.UtcNow;
            InsertAsync(guid, value, timestamp, timeSeries).Wait();
        }

        public async Task InsertAsync(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true)
        {
           
            if (timestamp == default) timestamp = DateTime.UtcNow;
            if (timestamp.Kind != DateTimeKind.Utc) timestamp = timestamp.ToUniversalTime();
            await Task.Run(() => _updateEntityQueue.Enqueue((guid, value, timestamp, timeSeries)));
            
        }

        #endregion Insert

        #region Get
        public (BsonValue Value, DateTime Timestamp) Get(string guid)
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
        private Entity? AddUpdateEntity(string guid, BsonValue value, DateTime timestamp, ILiteCollection<Entity> entities)
        {
            try
            {

                var entity = entities.FindOne(e => e.Guid == guid);
                if (entity == null)
                {
                    entity = new Entity { Guid = guid, Value = value, Timestamp = timestamp };
                    var id = entities.Insert(entity);
                   
                }
                else
                {
                    entity.Value = value;
                    entity.Timestamp = timestamp;
                    entities.Update(entity);
                }
                _entities[entity.Guid] = entity;
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
                    _entities[entity.Guid] = entity;
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
            public long Id { get; set; } = 0;
            public string Guid { get; set; } = string.Empty;
            public BsonValue Value { get; set; } = BsonValue.Null;
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        }

    }

}