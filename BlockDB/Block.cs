using IoTDBdotNET.Attributes;
using IoTDBdotNET.SystemTables;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace IoTDBdotNET.BlockDB
{
    public class Block
    {
        public Guid Id { get; set; }
        public BsonValue Data { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        public string PreviousHash { get; set; }
        public string Hash { get; set; }

        public Block() { }

        public Block(string previousHash, BsonValue data)
        {
            Timestamp = DateTime.UtcNow;
            PreviousHash = previousHash;
            Data = data;
            Hash = CalculateHash();
        }

        public string CalculateHash()
        {
            using (SHA256 sha256 = SHA256.Create())
            {
                string rawData = $"{Timestamp}-{PreviousHash ?? ""}-{Data}";
                byte[] bytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(rawData));

                StringBuilder builder = new StringBuilder();
                foreach (byte b in bytes)
                {
                    builder.Append(b.ToString("x2"));
                }
                return builder.ToString();
            }
        }
    }
}
