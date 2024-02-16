using IoTDB.NET.Base;
using LiteDB;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using TeaTime;

namespace IoTDB.NET
{
    internal class EntityDatabase : BaseDatabase, IEntityDatabase, IDisposable
    {
        
        // Define the event based on the delegate
        private ConcurrentQueue<(string guid, string name, BsonValue value, DateTime timestamp, bool isUniqueIdentifier)> _propertyValueQueue;
        private bool _queueProcessing = false;

        private ConcurrentDictionary<string, NewEntitty> _addEntitiesBuffer = new(); //key = uniqueHash and value = guid
        private ConcurrentDictionary<string, NewEntitty> _entitiesBuffered = new(); //key = uniqueHash and value = guid
        public int GetEntityQueueCount() { return _addEntitiesBuffer.Count; }
        public EntityDatabase(string dbPath) : base(dbPath)
        {
            try
            {
                this._propertyValueQueue = new();
                InitializeDatabase();
                StartBackgroundTask();
            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }

        }

        protected override void InitializeDatabase()
        {
            try
            {
                var entities = Database.GetCollection<Entity>("Entities");
                entities.EnsureIndex(x => x.Guid, true);

                var properties = Database.GetCollection<Property>("Properties");
                properties.EnsureIndex(x => x.Guid);
                properties.EnsureIndex(x => new { x.Guid, x.Name }, true);
            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
        }

        protected override void PerformBackgroundWork(CancellationToken token)
        {
            if (!token.IsCancellationRequested)
            {
                if (_addEntitiesBuffer.Count > 0 && !_queueProcessing)
                {
                    try
                    {
                        FlushAddEntitiesQueue();
                     
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                        lock (SyncRoot) { _queueProcessing = false; }
                    }
                }

                if (_addEntitiesBuffer.Count == 0)
                {
                    lock (SyncRoot)
                    {
                        string key = string.Empty;
                        foreach (var e in _entitiesBuffered)
                        {
                            if (IsEntityExists(e.Key))
                            {
                                key = e.Key;
                                
                            }
                            break;
                        }
                        _entitiesBuffered.TryRemove(key, out _);
                    }
                }
                if (_propertyValueQueue.Count > 0 && !_queueProcessing)
                {
                    try
                    {
                        FlushSetPropertyValueQueue();
                      
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                        lock (SyncRoot) { _queueProcessing = false; }
                    }
                }

            }

        }

        private void FlushSetPropertyValueQueue()
        {
            lock (SyncRoot) // Acquire the lock
            {
                _queueProcessing = true;
                try
                {

                    const int MaxItemsPerFlush = 1000; // Adjust this value as needed
                    List<Property> updateProperties = new List<Property>();
                    List<Property> addProperties = new List<Property>();
                    int itemsProcessed = 0;
                    var propertiesCollection = Database.GetCollection<Property>("Properties");
                    
                    while (itemsProcessed <= MaxItemsPerFlush && _propertyValueQueue.TryDequeue(out var item))
                    {
                        
                        var existingProperty = propertiesCollection.FindOne(p => p.Guid == item.guid && p.Name.Equals(item.name));

                        if (existingProperty == null)
                        {
                            var p = addProperties.FirstOrDefault(x => x.Guid == item.guid && x.Name.Equals(item.name));
                            if (p != null)
                            {
                                p.Value = item.value;
                                p.Timestamp = item.timestamp;
                                p.IsUniqueIdentifier = item.isUniqueIdentifier;
                            }
                            else
                            {
                                addProperties.Add(new Property() { Guid = item.guid, Name = item.name, Value = item.value, Timestamp = item.timestamp, IsUniqueIdentifier = item.isUniqueIdentifier });
                            }
                        }
                        else
                        {
                            if (existingProperty.IsUniqueIdentifier)
                            {
                                OnExceptionOccurred(new ExceptionEventArgs(new Exception("Invalid operation. Cannot set set Unique Identifier property.")));
                            }
                            else
                            {
                                //do not update IsUniqueIdentifier flag
                                var p = updateProperties.FirstOrDefault(x => x.Guid == item.guid && x.Name.Equals(item.name));
                                if (p != null)
                                {
                                    p.Value = item.value;
                                    p.Timestamp = item.timestamp;
                                }
                                else
                                {
                                    existingProperty.Value = item.value;
                                    existingProperty.Timestamp = item.timestamp;
                                    updateProperties.Add(existingProperty);
                                }
                            }
                        }
                        itemsProcessed++;
                    }

                    if (addProperties.Count > 0)
                    {
                        propertiesCollection.InsertBulk(addProperties, addProperties.Count);
                    }
                    if (updateProperties.Count > 0)
                    {
                        propertiesCollection.Update(updateProperties);
                    }

                }
                catch (Exception ex) { OnExceptionOccurred(new(ex)); }
                _queueProcessing = false;
            }
        }

        private void FlushAddEntitiesQueue()
        {
            lock (SyncRoot) // Acquire the lock
            {
                _queueProcessing = true;
                try
                {
                    const int MaxItemsPerFlush = 100; // Adjust this value as needed

                    int itemsProcessed = 0;
                    var entities = Database.GetCollection<Entity>("Entities");
                    var properties = Database.GetCollection<Property>("Properties");

                    List<Entity> addEntities = new List<Entity>();
                    List<Property> addProperties = new List<Property>();
                    List<KeyValuePair<string, NewEntitty>> removeItems = new ();
                    foreach (var item in _addEntitiesBuffer) {
                        if (itemsProcessed++ > MaxItemsPerFlush) break;
                       
                        if (!IsEntityExists(item.Key)) // key is unique hash
                        {
                            var entity = new Entity { Guid = item.Value.Guid }; //value is guid
                            item.Value.Id = entities.Insert(entity);
                            //addEntities.Add(entity);
                            // Assuming Property class and its properties for this example
                            var property = new Property
                            {
                                Guid = item.Value.Guid,
                                Name = PropertyName.UniqueIdentifier,
                                Value = item.Key,
                                Timestamp = item.Value.Timestamp,
                                IsUniqueIdentifier = true
                            };
                            addProperties.Add(property);
                            // Your existing logic for setting property values...
                            foreach (var identifier in item.Value.Properties)
                            {
                                var p = new Property
                                {
                                    Guid = item.Value.Guid,
                                    Name = identifier.PropertyName,
                                    Value = identifier.Value,
                                    Timestamp = item.Value.Timestamp,
                                    IsUniqueIdentifier = true
                                };
                                addProperties.Add(p);
                                
                            }
                            _entitiesBuffered.TryAdd(item.Key, item.Value);
                        }
                        removeItems.Add(item);
                        

                    }

                    //if (addEntities.Count > 0) entities.InsertBulk(addEntities, addEntities.Count);
                    if (addProperties.Count > 0) properties.InsertBulk(addProperties, addProperties.Count);
                    foreach (var item in removeItems)
                    {
                        _addEntitiesBuffer.TryRemove(item.Key, out _);
                    }
                }
                catch (Exception ex) { OnExceptionOccurred(new(ex)); }
                _queueProcessing = false;
            }
        }

        public void AddEntityQueue(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {
                _addEntitiesBuffer.TryAdd(HashUniqueIdentifiers(uniqueIdentifiers), new(uniqueIdentifiers));
            }
            catch (Exception ex)
            {
                OnExceptionOccurred(new(ex)); // Assuming this handles exceptions
            }
        }

        public void AddEntity(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {
                var entities = Database.GetCollection<Entity>("Entities");
                var properties = Database.GetCollection<Property>("Properties");

                var guid = Guid.NewGuid().ToString();

                if (!IsEntityExists(uniqueIdentifiers)) // Assume this method checks existence
                {
                    var entity = new Entity { Guid = guid };
                    entities.Insert(entity);

                    // Convert uniqueIdentifiers to BsonDocument using the new function
                    string identifiersDoc = HashUniqueIdentifiers(uniqueIdentifiers);

                    // Assuming Property class and its properties for this example
                    properties.Insert(new Property
                    {
                        Guid = guid,
                        Name = "UniqueIdentifiers", // Common property name for the identifiers
                        Value = identifiersDoc,
                        Timestamp = DateTime.UtcNow,
                        IsUniqueIdentifier = true
                    });

                    // Your existing logic for setting property values...
                    foreach (var identifier in uniqueIdentifiers)
                    {
                        SetPropertyValue(guid, identifier.PropertyName, identifier.Value, DateTime.UtcNow, true);
                    }
                }
            }
            catch (Exception ex)
            {
                OnExceptionOccurred(new(ex)); // Assuming this handles exceptions
            }
        }

        public void AddEntity(string guid)
        {
            try
            {
                var entities = Database.GetCollection<Entity>("Entities");

                if (!IsGuidExists(guid, entities))
                {
                    var entity = new Entity { Guid = guid };
                    entities.Insert(entity);
                }
                //_db.Commit(); do not need to do LiteDB auto commit
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }

        }

        public (long Id, string Guid)? GetEntity(long id)
        {
            try
            {

                var entities = Database.GetCollection<Entity>("Entities");

                // Find the entity by its Id
                var entity = entities.FindOne(e => e.Id == id);
                if (entity != null)
                {
                    // If the entity is found, return its Id and Guid
                    return (entity.Id, entity.Guid);
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return null; // If no entity is found, return null

        }

        public (long Id, string Guid)? FindEntity(string uniqueHash)
        {
            var properties = Database.GetCollection<Property>("Properties");
            var p = properties.FindOne(x=>x.Name.Equals(PropertyName.UniqueIdentifier) && x.Value.AsString.Equals(uniqueHash));
            if (p != null)
            {
                return GetEntity(p.Guid);
            }
            return null;
        }
        public (long Id, string Guid)? GetEntity(string guid)
        {
            try
            {

                var entities = Database.GetCollection<Entity>("Entities");

                // Find the entity by its Id
                var entity = entities.FindOne(e => e.Guid == guid);
                if (entity != null)
                {
                    // If the entity is found, return its Id and Guid
                    return (entity.Id, entity.Guid);
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return null; // If no entity is found, return null

        }

        public (long Id, string Guid)? GetEntity(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {

                var properties = Database.GetCollection<Property>("Properties");
                var entities = Database.GetCollection<Entity>("Entities");

                // First, find all GUIDs that match the uniqueIdentifiers criteria.
                var guids = new HashSet<string>();
                foreach (var (Key, Value) in uniqueIdentifiers)
                {
                    var matchingProperties = properties.Find(p => p.Name == Key && p.Value == Value);
                    foreach (var prop in matchingProperties)
                    {
                        guids.Add(prop.Guid);
                    }
                }

                // Now, find an entity that matches one of the found GUIDs.
                foreach (var guid in guids)
                {
                    var entity = entities.FindOne(e => e.Guid == guid);
                    if (entity != null)
                    {
                        // If an entity is found, return its Id and Guid.
                        return (entity.Id, entity.Guid);
                    } else
                    {
                        if (_entitiesBuffered.ContainsKey(guid))
                        {
                            return ((_entitiesBuffered[guid].Id, guid));
                        }
                    }
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            // If no matching entity is found, return null.
            return null;
        }


        /// <summary>
        /// Checks if a GUID exists in the Entities table.
        /// </summary>
        /// <param name="guid">The GUID to check for existence.</param>
        /// <returns>True if the GUID exists, false otherwise.</returns>
        public bool IsGuidExists(string guid)
        {
            try
            {

                var entities = Database.GetCollection<Entity>("Entities");
                return IsGuidExists(guid, entities);
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return false;
        }

        private bool IsGuidExists(string guid, ILiteCollection<Entity> entities)
        {
            // Use the Count method to check if any entities match the given GUID.
            int count = 0;
            try
            {
                count = entities.Count(e => e.Guid == guid);
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return count > 0;
        }

        private bool IsEntityExists(string uniqueHash)
        {
            try
            {
                var properties = Database.GetCollection<Property>("Properties");
                int count = properties.Count(x => x.Name.Equals(PropertyName.UniqueIdentifier) && x.Value.AsString.Equals(uniqueHash));
                return count > 0;

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            // Check if there's at least one GUID that has a matching count equal to the number of unique identifiers.
            // This means the entity has all the specified properties.
            //return guidMatches.Any(g => g.Value == uniqueIdentifiers.Length);
            return false;
        }
        private bool IsEntityExists((string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {

            return IsEntityExists(HashUniqueIdentifiers(uniqueIdentifiers));
            
        }

        public void SetPropertyValue(string guid, string propertyName, BsonValue value, DateTime timestamp)
        {
            SetPropertyValue(guid, propertyName, value, timestamp, false);
        }

        private void SetPropertyValue(string guid, string propertyName, BsonValue value, DateTime timestamp, bool isUniqueIdentifier)
        {
            if (timestamp.Kind != DateTimeKind.Utc) timestamp = timestamp.ToUniversalTime();
            _propertyValueQueue.Enqueue((guid, propertyName, value, timestamp, isUniqueIdentifier));
        }

        // Method to asynchronously get all properties for a specific GUID
        public List<(string PropertyName, BsonValue Value, DateTime Timestamp)> GetProperties(string guid)
        {
            var properties = new List<(string PropertyName, BsonValue Value, DateTime Timestamp)>();
            try
            {

                var propsCollection = Database.GetCollection<Property>("Properties");
                var results = propsCollection.Find(p => p.Guid == guid);
                properties = results.Select(p => (p.Name, p.Value, p.Timestamp)).ToList();


            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return properties;
        }

        public List<(string PropertyName, BsonValue Value, DateTime Timestamp)> GetProperties(string guid, params string[] propertyNames)
        {
            var properties = new List<(string PropertyName, BsonValue Value, DateTime Timestamp)>();
            try
            {
                var propsCollection = Database.GetCollection<Property>("Properties");

                // Optimize the query based on whether propertyNames are provided
                IEnumerable<Property> results = propsCollection.Find(p => p.Guid == guid && propertyNames.Contains(p.Name));

                // Convert the results to the desired list of tuples format
                properties = results.Select(p => (p.Name, p.Value, p.Timestamp)).ToList();
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return properties;
        }


        // Method to asynchronously update an entity's GUID
        public void UpdateEntityGuid(long id, string newGuid)
        {
            try
            {


                var entities = Database.GetCollection<Entity>("Entities");
                var properties = Database.GetCollection<Property>("Properties");

                var entity = entities.FindOne(e => e.Id == id);
                if (entity != null)
                {
                    entity.Guid = newGuid;
                    entities.Update(entity);

                    var propsToUpdate = properties.Find(p => p.Guid == entity.Guid);
                    foreach (var prop in propsToUpdate)
                    {
                        prop.Guid = newGuid;
                        properties.Update(prop);
                    }
                }
                //_db.Commit(); do not need to do LiteDB auto commit

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        public void DeleteEntity(long id)
        {
            try
            {

                var entities = Database.GetCollection<Entity>("Entities");
                var properties = Database.GetCollection<Property>("Properties");

                // First, find the entity to get its GUID
                var entity = entities.FindById(id);
                if (entity != null)
                {
                    // Delete the entity by ID
                    entities.Delete(id);

                    // Then, delete all properties associated with the entity's GUID
                    properties.DeleteMany(p => p.Guid == entity.Guid);
                }
                //_db.Commit(); do not need to do LiteDB auto commit

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }


        public void DeleteEntity(string guid)
        {
            try
            {

                var entities = Database.GetCollection<Entity>("Entities");
                var properties = Database.GetCollection<Property>("Properties");

                // Delete the entity by GUID
                var entityDeleted = entities.DeleteMany(e => e.Guid == guid) > 0;

                if (entityDeleted)
                {
                    // Delete all properties associated with the GUID
                    properties.DeleteMany(p => p.Guid == guid);
                }
                //_db.Commit(); do not need to do LiteDB auto commit

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        



        // Helper classes to represent documents in LiteDB
        private class Entity
        {
            public long Id { get; set; }
            public string Guid { get; set; }
        }

        private class Property
        {
            public long Id { get; set; }
            public string Guid { get; set; }
            public string Name { get; set; }
            public BsonValue Value { get; set; }
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
            public bool IsUniqueIdentifier { get; set; } = false;
        }

        private class NewEntitty
        {
            public long Id { get; set; }
            public string Guid { get; set; } = System.Guid.NewGuid().ToString();
            public string UniqueHash { get; set; }
            public List<(string PropertyName, BsonValue Value)> Properties { get; set; } = new();
            public DateTime Timestamp = DateTime.UtcNow;

            public NewEntitty(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
            {
                UniqueHash = BaseDatabase.HashUniqueIdentifiers(uniqueIdentifiers);
                Properties = uniqueIdentifiers.Select(p => (p.PropertyName, p.Value)).ToList();
            }
        }
    }

}