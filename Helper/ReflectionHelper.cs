using IoTDBdotNET.Attributes;
using IoTDBdotNET.TableDB;
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
        public static IEnumerable<PropertyInfo> GetPropertiesWithAttribute<T, A>() where A : Attribute
        {
            return GetPropertiesWithAttribute<A>(typeof(T));
        }
        

        public static IEnumerable<PropertyInfo> GetPropertiesWithAttribute<A>(Type type) where A : Attribute
        {
            foreach (var prop in type.GetProperties())
            {
                if (Attribute.IsDefined(prop, typeof(A)))
                {
                    yield return prop;
                }
            }


        }

        public static IEnumerable<ColumnInfo> GetTypeColumnsWithAttribute<A>(Type type) where A : Attribute
        {
            List<ColumnInfo> columns = new List<ColumnInfo>();
            var props = GetPropertiesWithAttribute<A>(type);
            foreach (var prop in props)
            {
                var attribute = prop.GetCustomAttribute<A>();
                if (attribute != null)
                {
                    columns.Add(new(prop, attribute));
                }
            }
            return columns;
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
