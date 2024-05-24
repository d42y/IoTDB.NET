using IoTDBdotNET.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp
{
    public class Address
    {
        public Guid Id { get; set; }
        [TableForeignKey(typeof(Friend), TableConstraint.Cascading, RelationshipOneTo.One, "Each friend only have one address." )]
        public Guid FriendId { get; set; }
        public string Street { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string ZipCode { get; set; }
    }
}
