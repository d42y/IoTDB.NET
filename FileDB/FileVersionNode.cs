using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class FileVersionNode
    {
        public int Version { get; set; }
        public List<FileVersionNode> Children { get; set; } = new List<FileVersionNode>();
    }
}
