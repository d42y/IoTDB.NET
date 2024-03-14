using IoTDBdotNET.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.TableDB
{
    public class TableInfo
    {
        public string Name { get; set; }
        public ColumnInfo? Id { get; set; }
        public List<ColumnInfo> Uniques { get; set; } = new();
        public List<ColumnInfo> ForeignKeys { get; set; } = new();
        public List<ColumnInfo> Columns { get; set; } = new();
        public List<ColumnInfo> ForeignTables { get; set; } = new();
        public List<TableInfo> ChildTables { get; set; } = new();

        public TableInfo() { }
        public TableInfo(Type type) 
        {
            Name = type.Name;

            //Id
            var id = BaseDatabase.GetIdProperty(type);
            if (id == null) Id = null;
            else Id = new(id);
            Uniques = Helper.ReflectionHelper.GetTypeColumnsWithAttribute<UniqueValueAttribute>(type).ToList();
            ForeignKeys = Helper.ReflectionHelper.GetTypeColumnsWithAttribute<TableForeignKeyAttribute>(type).ToList();

            PropertyInfo[] properties = type.GetProperties();
            foreach (var property in properties)
            {

                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    Type refTableType = property.PropertyType.GetGenericArguments()[0];
                    if (Id != null && BaseDatabase.GetRefTableIdProperty(type, refTableType) != null)
                    {
                        if (property.Name.Equals($"{refTableType.Name}Table"))
                        {
                            ForeignTables.Add(new(property));
                        }

                    }

                }
            }
            foreach (var property in properties)
            {
                if (property.Name != "Id" && !Uniques.Any(x=>x.Name == property.Name) 
                    && !ForeignKeys.Any(x=>x.Name == property.Name) && !ForeignTables.Any(x=>x.Name == property.Name))
                {
                    Columns.Add(new(property));
                }
            }
        }
    }
}
