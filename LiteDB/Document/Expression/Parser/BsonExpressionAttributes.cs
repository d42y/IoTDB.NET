﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using static IoTDBdotNET.Constants;

namespace IoTDBdotNET
{
    /// <summary>
    /// When a method are decorated with this attribute means that this method are not immutable
    /// </summary>
    internal class VolatileAttribute: Attribute
    {
    }
}
