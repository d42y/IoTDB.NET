using LiteDB;

namespace IoTDB.NET
{
    public interface IEntityDatabase
    {
        event EventHandler<ExceptionEventArgs> ExceptionOccurred;

        void AddEntity(params (string Key, BsonValue Value)[] uniqueIdentifiers);
        void AddOrUpdateProperties(string guid, params (string Key, BsonValue Value)[] properties);
        void DeleteEntity(long id);
        void DeleteEntity(string guid);
        Dictionary<string, BsonValue> GetProperties(string guid);
        (long Id, string Guid)? GetEntity(long id);
        (long Id, string Guid)? GetEntity(string guid);
        (long Id, string Guid)? GetEntity(params (string Key, BsonValue Value)[] uniqueIdentifiers);
        Dictionary<string, BsonValue> GetProperties(string guid, params string[] keys);
        bool IsGuidExists(string guid);
        void UpdateEntityGuid(long id, string newGuid);
    }
}