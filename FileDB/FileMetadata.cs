using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class FileMetadata
    {
        public Guid Id { get; set; }
        public string FileName { get; set; }
        public string FileExtension { get; set; }
        public int CurrentVersion { get; set; } = 0;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        public FileMetadata() { }
    }
}
