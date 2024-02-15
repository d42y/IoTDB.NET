using LiteDB;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using TeaTime;

namespace IoTDB.NET
{
    internal class EntityDatabase : IEntityDatabase, IDisposable
    {

        // Define the event based on the delegate
        public event EventHandler<ExceptionEventArgs> ExceptionOccurred;
        private readonly LiteDatabase _db;

        private ConcurrentQueue<(string guid, string name, BsonValue value, DateTime timestamp, bool isUniqueIdentifier)> _propertyValueQueue;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private Task? writeTask;
        private readonly object _syncRoot = new object();
        private bool _queueProcessing = false;

        public EntityDatabase(string dbPath)
        {
            try
            {
                _db = new LiteDatabase(dbPath);
                this._propertyValueQueue = new();
                InitializeDatabase();
                StartBackgroundTask();
            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
            
        }

        private void InitializeDatabase()
        {
            try
            {
                var entities = _db.GetCollection<Entity>("Entities");
                entities.EnsureIndex(x => x.Guid, true);

                var properties = _db.GetCollection<Property>("Properties");
                properties.EnsureIndex(x => x.Guid);
                properties.EnsureIndex(x => new { x.Guid, x.Name }, true);
            } catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
        }

        private void StartBackgroundTask()
        {
            this.writeTask = Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    if (_propertyValueQueue.Count > 0 && !_queueProcessing)
                    {
                        try
                        {
                            FlushSetPropertyValueQueue();
                        }
                        catch (Exception ex)
                        {
                            OnExceptionOccurred(new(ex));
                            lock(_syncRoot) { _queueProcessing = false; }
                        }
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(100), _cancellationTokenSource.Token);
                }
            }, _cancellationTokenSource.Token);
        }

        private void FlushSetPropertyValueQueue()
        {
            lock (_syncRoot) // Acquire the lock
            {
                _queueProcessing = true;
                try
                {

                    const int MaxItemsPerFlush = 1000; // Adjust this value as needed
                    List<Property> updateProperties = new List<Property>();
                    List<Property> addProperties = new List<Property>();
                    int itemsProcessed = 0;
                    var propertiesCollection = _db.GetCollection<Property>("Properties");
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
                            var p = updateProperties.FirstOrDefault(x => x.Guid == item.guid && x.Name.Equals(item.name));
                            if (p != null)
                            {
                                p.Value = item.value;
                                p.Timestamp = item.timestamp;
                                p.IsUniqueIdentifier = item.isUniqueIdentifier;
                            }
                            else
                            {
                                existingProperty.Value = item.value;
                                existingProperty.Timestamp = item.timestamp;
                                existingProperty.IsUniqueIdentifier |= item.isUniqueIdentifier;
                                updateProperties.Add(existingProperty);
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

        private BsonDocument ConvertUniqueIdentifiersToBsonDocument(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            BsonDocument document = new BsonDocument();

            foreach (var identifier in uniqueIdentifiers)
            {
                document[identifier.PropertyName] = identifier.Value;
            }

            return document;
        }

        private string GenerateUniqueHashFromIdentifiers(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            // Step 1: Concatenate the property names and values into a single string
            StringBuilder stringBuilder = new StringBuilder();
            foreach (var identifier in uniqueIdentifiers)
            {
                stringBuilder.Append(identifier.PropertyName);
                stringBuilder.Append("=");
                stringBuilder.Append(identifier.Value.ToString());
                stringBuilder.Append(";");
            }

            // Step 2: Convert the concatenated string to a byte array
            byte[] byteData = Encoding.UTF8.GetBytes(stringBuilder.ToString());

            // Step 3: Use a hash algorithm to generate a hash from the byte array
            using (SHA256 sha256Hash = SHA256.Create())
            {
                byte[] hashData = sha256Hash.ComputeHash(byteData);

                // Step 4: Convert the byte array hash to a hexadecimal string
                StringBuilder hashStringBuilder = new StringBuilder();
                for (int i = 0; i < hashData.Length; i++)
                {
                    hashStringBuilder.Append(hashData[i].ToString("x2")); // "x2" for lowercase hex format
                }

                return hashStringBuilder.ToString();
            }
        }

        // Usage within your AddEntity method or elsewhere
        public void AddEntity(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {
                var entities = _db.GetCollection<Entity>("Entities");
                var properties = _db.GetCollection<Property>("Properties");

                var guid = Guid.NewGuid().ToString();

                if (!IsEntityExists(uniqueIdentifiers)) // Assume this method checks existence
                {
                    var entity = new Entity { Guid = guid };
                    entities.Insert(entity);

                    // Convert uniqueIdentifiers to BsonDocument using the new function
                    string identifiersDoc = GenerateUniqueHashFromIdentifiers(uniqueIdentifiers);

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
                
                var entities = _db.GetCollection<Entity>("Entities");

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
                
                var entities = _db.GetCollection<Entity>("Entities");

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


        public (long Id, string Guid)? GetEntity(string guid)
        {
            try
            {
                
                var entities = _db.GetCollection<Entity>("Entities");

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
               
                var properties = _db.GetCollection<Property>("Properties");
                var entities = _db.GetCollection<Entity>("Entities");

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
                
                var entities = _db.GetCollection<Entity>("Entities");
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

        private bool IsEntityExists((string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {

            // This dictionary will hold the count of matching properties for each GUID.
            //var guidMatches = new Dictionary<string, long>();
            try
            {
                var properties = _db.GetCollection<Property>("Properties");
                int count = properties.Count(x => x.Name.Equals(PropertyName.UniqueIdentifier) && x.Value.Equals(GenerateUniqueHashFromIdentifiers(uniqueIdentifiers)));
                return count > 0;
                //foreach (var identifier in uniqueIdentifiers)
                //{
                //    // Find properties that match the current identifier.
                //    var matchingProperties = properties.Find(p => p.Name == identifier.PropertyName && p.Value == identifier.Value);

                //    foreach (var property in matchingProperties)
                //    {
                //        // If the GUID is already in the dictionary, increment its count, otherwise add it with a count of 1.
                //        if (guidMatches.ContainsKey(property.Guid))
                //        {
                //            guidMatches[property.Guid]++;
                //        }
                //        else
                //        {
                //            guidMatches[property.Guid] = 1;
                //        }
                //    }
                //}
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            // Check if there's at least one GUID that has a matching count equal to the number of unique identifiers.
            // This means the entity has all the specified properties.
            //return guidMatches.Any(g => g.Value == uniqueIdentifiers.Length);
            return false;
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
        // Method to add or update properties for a specific GUID
        //public void AddOrUpdateProperties(string guid, params (string PropertyName, BsonValue Value)[] properties)
        //{
        //    try
        //    {
        //        var propertiesCollection = _db.GetCollection<Property>("Properties");

        //        // Retrieve all existing properties for the given GUID in one go
        //        var existingProperties = propertiesCollection.Find(p => p.Guid == guid).ToList();

        //        // Prepare lists for bulk operations
        //        var propertiesToUpdate = new List<Property>();
        //        var propertiesToInsert = new List<Property>();

        //        foreach (var property in properties)
        //        {
        //            var existingProperty = existingProperties.FirstOrDefault(p => p.Name == property.PropertyName);

        //            if (existingProperty != null)
        //            {
        //                // Prepare existing property for update
        //                existingProperty.Value = property.Value;
        //                propertiesToUpdate.Add(existingProperty);
        //            }
        //            else
        //            {
        //                // Prepare new property for insertion
        //                propertiesToInsert.Add(new Property
        //                {
        //                    Guid = guid,
        //                    Name = property.PropertyName,
        //                    Value = property.Value
        //                });
        //            }
        //        }

        //        // Perform bulk update and insert
        //        if (propertiesToUpdate.Any())
        //        {
        //            propertiesCollection.Update(propertiesToUpdate);
        //        }

        //        if (propertiesToInsert.Any())
        //        {
        //            propertiesCollection.InsertBulk(propertiesToInsert);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        OnExceptionOccurred(new(ex));
        //    }
        //}


        // Method to asynchronously get all properties for a specific GUID
        public Dictionary<string, BsonValue> GetProperties(string guid)
        {
            var properties = new Dictionary<string, BsonValue>();
            try
            {

                var propsCollection = _db.GetCollection<Property>("Properties");
                var results = propsCollection.Find(p => p.Guid == guid);

                foreach (var item in results)
                {
                    properties[item.Name] = item.Value;
                }

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return properties;
        }

        public List<(string PropertyName, BsonValue Value)> GetProperties(string guid, params string[] propertyNames)
        {
            var properties = new List<(string PropertyName, BsonValue Value)>();
            try
            {
                var propsCollection = _db.GetCollection<Property>("Properties");

                // Optimize the query based on whether propertyNames are provided
                IEnumerable<Property> results = propsCollection.Find(p => p.Guid == guid && propertyNames.Contains(p.Name));
                
                // Convert the results to the desired list of tuples format
                properties = results.Select(p => (p.Name, p.Value)).ToList();
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return properties;
        }


        // Method to asynchronously update an entity's GUID
        public void UpdateEntityGuid(long id, string newGuid)
        {
            try
            {
                

                var entities = _db.GetCollection<Entity>("Entities");
                var properties = _db.GetCollection<Property>("Properties");

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
                
                var entities = _db.GetCollection<Entity>("Entities");
                    var properties = _db.GetCollection<Property>("Properties");

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
                
                var entities = _db.GetCollection<Entity>("Entities");
                    var properties = _db.GetCollection<Property>("Properties");

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


        // Method to raise the event
        protected virtual void OnExceptionOccurred(ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(this, e);
        }

        public void Dispose()
        {
            _db?.Dispose();
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
    }

}