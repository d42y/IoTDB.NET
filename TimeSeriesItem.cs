using IoTDBdotNET;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET
{
    public struct TimeSeriesItem
    {
        public string Guid { get; set; }
        public long EntityIndex { get; set; }
        public BsonValue Value { get; set; }
        public DateTime Timestamp { get; set; }

        public DateTime ToLocalDateTime => Timestamp.ToLocalTime();
    }
}
