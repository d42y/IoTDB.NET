

using System.Runtime.InteropServices;

namespace IoTDBdotNET.Helper
{
    internal static class MachineInfo
    {
        public static ulong GetTotalPhysicalMemoryInMB()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return GetTotalPhysicalMemoryInMBWindows();
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return GetTotalPhysicalMemoryInMBLinux();
            }
            else
            {
                throw new PlatformNotSupportedException("Unsupported operating system. Only Windows and Linux are supported.");
            }
        }

        private static ulong GetTotalPhysicalMemoryInMBWindows()
        {
            // Windows implementation (using P/Invoke as previously described)
            MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
            memStatus.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            if (GlobalMemoryStatusEx(ref memStatus))
            {
                return memStatus.ullTotalPhys / 1024 / 1024;
            }
            return 0;
        }

        private static ulong GetTotalPhysicalMemoryInMBLinux()
        {
            // Linux implementation (reading from /proc/meminfo as previously described)
            try
            {
                string memInfo = File.ReadAllText("/proc/meminfo");
                string[] lines = memInfo.Split('\n');
                foreach (var line in lines)
                {
                    if (line.StartsWith("MemTotal:"))
                    {
                        string[] parts = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        ulong memKb = ulong.Parse(parts[1]); // Memory in KB
                        return memKb / 1024; // Convert to MB
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Out.WriteLineAsync($"An error occurred: {ex.Message}");
                return 0;
            }

            return 0;
        }

        // P/Invoke declaration for Windows
        [DllImport("kernel32.dll")]
        private static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

        [StructLayout(LayoutKind.Sequential)]
        private struct MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
        }

        public static void CreateDirectory(string dbPathName)
        {
            if (!Directory.Exists(dbPathName))
            {
                Directory.CreateDirectory(dbPathName);
            }
        }
    }
}
