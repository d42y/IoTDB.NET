
namespace IoTDBdotNET
{
    public interface ITimeSeriesDatabase
    {
        event EventHandler<ExceptionEventArgs> ExceptionOccurred;
        void DeleteEntity(string guid);
        Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to);
        Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to, int interval, IntervalType intervalType);
        (BsonValue Value, DateTime Timestamp) Get(string guid);
        void Insert(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true);
        Task InsertAsync(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true);
        void UpdateEntityGuid(long id, string newGuid);
    }
}