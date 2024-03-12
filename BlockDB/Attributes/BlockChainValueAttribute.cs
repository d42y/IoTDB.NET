using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.BlockDB.Attributes
{
    [AttributeUsage(AttributeTargets.Property)] // Apply to properties only.
    public class BlockChainValueAttribute : Attribute
    {
        public string Description { get; set; }

        public BlockChainValueAttribute(string description = "")
        {
            Description = description;
        }
    }
}
