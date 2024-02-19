
namespace IoTDBdotNET
{
    public interface ITimeSeriesDatabase
    {
        event EventHandler<ExceptionEventArgs> ExceptionOccurred;
        void DeleteEntity(string guid);
        Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to);
        Task<Dictionary<string, List<TimeSeriesItem>>> GetAsync(List<string> guids, DateTime from, DateTime to, int interval, IntervalType intervalType);
        Task<(BsonValue Value, DateTime Timestamp)> GetAsync(string guid);
        void Set(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true);
        Task SetAsync(string guid, BsonValue value, DateTime timestamp = default, bool timeSeries = true);
        void UpdateEntityGuid(long id, string newGuid);
    }
}