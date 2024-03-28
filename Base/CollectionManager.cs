using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Base
{
    internal class CollectionManager<T> where T : class
    {
        public string CollectionName { get; set; }
        public ILiteCollection<T> Collection { get; set; }

        public CollectionManager(string collectionName)
        {
            CollectionName = collectionName;
        }
    }
}
