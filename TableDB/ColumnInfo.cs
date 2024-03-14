using IoTDBdotNET.Attributes;
using System.Reflection;

namespace IoTDBdotNET.TableDB
{
    public class ColumnInfo
    {
        public string Name { get; set; }
        public Attribute? Attribute { get; set; }
        public PropertyInfo PropertyInfo { get; set; }
        public ColumnInfo() { }
        public ColumnInfo(PropertyInfo propertyInfo, Attribute? attribute = null)
        {
            Name = propertyInfo.Name;
            Attribute = attribute;
            PropertyInfo = propertyInfo;
        }

    }
}
