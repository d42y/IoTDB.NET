using LiteDB;

namespace IoTDB.NET
{
    internal class EntityDatabase : IEntityDatabase, IDisposable
    {

        // Define the event based on the delegate
        public event EventHandler<ExceptionEventArgs> ExceptionOccurred;
        private readonly LiteDatabase _db;
        public EntityDatabase(string dbPath)
        {
            try
            {
                _db = new LiteDatabase(dbPath);
            }
            catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
            InitializeDatabase(); // Synchronously initialize the database
        }

        private void InitializeDatabase()
        {
            try
            {
                var entities = _db.GetCollection<Entity>("Entities");
                entities.EnsureIndex(x => x.Guid, true);

                var properties = _db.GetCollection<Property>("Properties");
                properties.EnsureIndex(x => x.Guid);
                properties.EnsureIndex(x => new { x.Guid, x.Key }, true);
            } catch (Exception ex) { throw new Exception($"Failed to initialize database. {ex.Message}"); }
        }

        public void AddEntity(params (string Key, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {
                var entities = _db.GetCollection<Entity>("Entities");
                var properties = _db.GetCollection<Property>("Properties");

                var guid = Guid.NewGuid().ToString();

                if (!IsEntityExists(uniqueIdentifiers)) // Implement this method based on unique identifiers logic
                {
                    var entity = new Entity { Guid = guid };
                    entities.Insert(entity);

                    foreach (var (Key, Value) in uniqueIdentifiers)
                    {
                        properties.Insert(new Property { Guid = guid, Key = Key, Value = Value });
                    }
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }

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

        public (long Id, string Guid)? GetEntity(params (string Key, BsonValue Value)[] uniqueIdentifiers)
        {
            try
            {
               
                var properties = _db.GetCollection<Property>("Properties");
                var entities = _db.GetCollection<Entity>("Entities");

                // First, find all GUIDs that match the uniqueIdentifiers criteria.
                var guids = new HashSet<string>();
                foreach (var (Key, Value) in uniqueIdentifiers)
                {
                    var matchingProperties = properties.Find(p => p.Key == Key && p.Value == Value);
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

        private bool IsEntityExists((string Key, BsonValue Value)[] uniqueIdentifiers)
        {

            // This dictionary will hold the count of matching properties for each GUID.
            var guidMatches = new Dictionary<string, long>();
            try
            {
                var properties = _db.GetCollection<Property>("Properties");
                foreach (var identifier in uniqueIdentifiers)
                {
                    // Find properties that match the current identifier.
                    var matchingProperties = properties.Find(p => p.Key == identifier.Key && p.Value == identifier.Value);

                    foreach (var property in matchingProperties)
                    {
                        // If the GUID is already in the dictionary, increment its count, otherwise add it with a count of 1.
                        if (guidMatches.ContainsKey(property.Guid))
                        {
                            guidMatches[property.Guid]++;
                        }
                        else
                        {
                            guidMatches[property.Guid] = 1;
                        }
                    }
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            // Check if there's at least one GUID that has a matching count equal to the number of unique identifiers.
            // This means the entity has all the specified properties.
            return guidMatches.Any(g => g.Value == uniqueIdentifiers.Length);
        }

        // Method to add or update properties for a specific GUID
        public void AddOrUpdateProperties(string guid, params (string Key, BsonValue Value)[] properties)
        {
            try
            {
                
                var propertiesCollection = _db.GetCollection<Property>("Properties");

                foreach (var property in properties)
                {
                    // Check if the property already exists
                    var existingProperty = propertiesCollection.FindOne(p => p.Guid == guid && p.Key == property.Key);

                    if (existingProperty != null)
                    {
                        // Update existing property
                        existingProperty.Value = property.Value;
                        propertiesCollection.Update(existingProperty);
                    }
                    else
                    {
                        // Insert new property
                        var newProperty = new Property
                        {
                            Guid = guid,
                            Key = property.Key,
                            Value = property.Value
                        };
                        propertiesCollection.Insert(newProperty);
                    }
                }
                //_db.Commit(); do not need to do LiteDB auto commit
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

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
                    properties[item.Key] = item.Value;
                }

            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return properties;
        }

        public Dictionary<string, BsonValue> GetProperties(string guid, params string[] keys)
        {
            var properties = new Dictionary<string, BsonValue>();
            try
            {

                var propsCollection = _db.GetCollection<Property>("Properties");
                foreach (var key in keys)
                {
                    var result = propsCollection.FindOne(p => p.Guid == guid && p.Key == key);
                    if (result != null)
                    {
                        properties[result.Key] = result.Value;
                    }
                }

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
            public string Key { get; set; }
            public BsonValue Value { get; set; }
        }
    }

}