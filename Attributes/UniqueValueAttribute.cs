using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Attributes
{
    [AttributeUsage(AttributeTargets.Property)] // Apply to properties only.
    public class UniqueValueAttribute : Attribute
    {
        public string Description { get; set; }

        public UniqueValueAttribute(string description = "")
        {
            Description = description;
        }
    }
}
