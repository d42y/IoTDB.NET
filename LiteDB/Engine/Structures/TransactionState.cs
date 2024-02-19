using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using static IoTDBdotNET.Constants;

namespace IoTDBdotNET.Engine
{
    internal enum TransactionState
    {
        Active,
        Committed,
        Aborted,
        Disposed
    }
}