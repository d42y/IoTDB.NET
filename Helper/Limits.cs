using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Helper
{
    internal static class Limits
    {
        public static int GetMaxProcessingItems()
        {
            ulong totalMem = Helper.MachineInfo.GetTotalPhysicalMemoryInMB();
            double factor = 5000.0 / 65536.0;
            int limit = (int)(totalMem * factor);
            if (limit < 500) limit = 500;

            return limit;
        }
    }
}
