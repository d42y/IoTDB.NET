using LiteDB;
using TeaTime;

namespace IoTDB.NET
{
    // Time series item structure
    public struct TimeSeriesItem
    {
        public long Id { get; set; }
        public long EntityIndex { get; set; }
        public BsonValue Value { get; set; }
        public DateTime Timestamp { get; set; }

        public DateTime ToLocalDateTime { get { return ((DateTime)Timestamp).ToLocalTime(); } }
    }
}
