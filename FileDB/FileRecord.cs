using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class FileRecord
    {
        public FileMetadata Metadata { get; set; }
        public List<FileAccessLog> AccessLog { get; set; } = new List<FileAccessLog>();
        public List<FileCheckoutRecord> CheckoutRecords { get; set; } = new List<FileCheckoutRecord>();
        public List<FileVersionNode> FileVersions { get; set; } = new List<FileVersionNode>();

    }
}
