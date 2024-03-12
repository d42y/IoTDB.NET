using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.BlockDB.Attributes
{
    public class BlockInfo
    {
        public string Name { get; set; }
        public string AttributeName { get; set; }
        public string AttributeDescription { get; set; }
        public PropertyInfo Property { get; set; }
        public BlockInfo() { }
        public BlockInfo(string name, string attributeName, string attributeDescription, PropertyInfo propertyInfo)
        {
            Name = name;
            AttributeName = attributeName;
            AttributeDescription = attributeDescription;
            Property = propertyInfo;
        }
    }
}
