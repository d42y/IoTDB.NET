﻿using System;
using static IoTDBdotNET.Constants;

namespace IoTDBdotNET
{
    /// <summary>
    /// Indicate that property will be used as BsonDocument Id
    /// </summary>
    public class BsonIdAttribute : Attribute
    {
        public bool AutoId { get; private set; }

        public BsonIdAttribute()
        {
            this.AutoId = true;
        }

        public BsonIdAttribute(bool autoId)
        {
            this.AutoId = autoId;
        }
    }
}