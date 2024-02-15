using LiteDB;

namespace IoTDB.NET
{
    public interface IEntityDatabase
    {
        event EventHandler<ExceptionEventArgs> ExceptionOccurred;

        void AddEntity(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers);
        void SetPropertyValue(string guid, string propertyName, BsonValue value, DateTime timestamp);
        //void AddOrUpdateProperties(string guid, params (string PropertyName, BsonValue Value)[] properties);
        void DeleteEntity(long id);
        void DeleteEntity(string guid);
        Dictionary<string, BsonValue> GetProperties(string guid);
        (long Id, string Guid)? GetEntity(long id);
        (long Id, string Guid)? GetEntity(string guid);
        (long Id, string Guid)? GetEntity(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers);
        Dictionary<string, BsonValue> GetProperties(string guid, params string[] propertyNames);
        bool IsGuidExists(string guid);
        void UpdateEntityGuid(long id, string newGuid);
    }
}