using IoTDBdotNET;

namespace IoTDBdotNET
{
    // Time series item structure
    internal struct TSItem
    {
        public long Id { get; set; }
        public long EntityIndex { get; set; }
        public BsonValue Value { get; set; }
        public DateTime Timestamp { get; set; }

        public DateTime ToLocalDateTime { get { return Timestamp.ToLocalTime(); } }
    }
}
