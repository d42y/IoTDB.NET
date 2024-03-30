using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class FileCheckoutRecord
    {
        public Guid Id { get; set; }
        public Guid FileId { get; set; }
        public int CheckoutVersion { get; set; }
        public int CheckinVersion { get; set;}
        public string Username { get; set; }
        public DateTime Timestamp { get; set; }
        public FileCheckoutStatus Status { get; set; }
    }
}
