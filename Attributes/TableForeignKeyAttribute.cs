using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Attributes
{
    [AttributeUsage(AttributeTargets.Property)] // Apply to properties only.
    public class TableForeignKeyAttribute : Attribute
    {
        public Type Type { get; set; }
        public RelationshipOneTo RelationshipOneTo { get; set; }
        public string Description { get; set; }

        public TableForeignKeyAttribute(Type type, RelationshipOneTo relationshipOneTo = RelationshipOneTo.Many, string description = "")
        {
            Type = type;
            RelationshipOneTo = relationshipOneTo;
            Description = description;
        }
    }
}
