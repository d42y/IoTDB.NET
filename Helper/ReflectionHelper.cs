using IoTDBdotNET.BlockDB.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Helper
{
    public static class ReflectionHelper
    {
        public static IEnumerable<PropertyInfo> GetPropertiesWithBlockChainValueAttribute<T>()
        {
            return GetPropertiesWithBlockChainValueAttribute(typeof(T));
        }
        public static IEnumerable<PropertyInfo> GetPropertiesWithBlockChainValueAttribute(Type type)
        {
            foreach (var prop in type.GetProperties())
            {
                if (Attribute.IsDefined(prop, typeof(BlockChainValueAttribute)))
                {
                    yield return prop;
                }
            }

            
        }

        public static PropertyInfo? GetProperty (Type type, string name)
        {
            foreach (var prop in type.GetProperties())
            {
                if (prop.Name == name)
                {
                    return prop;
                }
            }
            return null;
        }
    }
}
