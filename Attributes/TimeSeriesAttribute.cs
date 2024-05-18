using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class TimeSeriesAttribute : Attribute
    {
        public string Description { get; set; } = string.Empty;
        public TimeSeriesAttribute() { }
        public TimeSeriesAttribute(string description)
        {
            Description = description;
        }
    }
}
