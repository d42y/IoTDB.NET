using System;
using static IoTDBdotNET.Constants;

namespace IoTDBdotNET
{
    /// <summary>
    /// Indicate that property will not be persist in Bson serialization
    /// </summary>
    public class BsonIgnoreAttribute : Attribute
    {
    }
}