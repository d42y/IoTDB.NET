using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class FileClass
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public int Revision { get; set; } = 1;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        public FileClass() { }
    }
}
