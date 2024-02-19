using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET
{
    public static class PropertyName
    {
        //default values
        public static readonly string Name = "Name";
        public static readonly string Description = "Description";
        public static readonly string UniqueIdentifier = "UniqueIdentifier";
        public static readonly string ParentIdentifier = "ParentIdentifier";
        public static readonly string Location = "Location";
        public static readonly string Value = "Value";
        public static readonly string Timestamp = "Timestamp";
        // common BACnet properties
        public static readonly string ObjectIdentifier = "ObjectIdentifier";
        public static readonly string ObjectName = "ObjectName";
        public static readonly string ObjectType = "ObjectType";
        public static readonly string VendorName = "VendorName";
        public static readonly string VendorIdentifier = "VendorIdentifier";
        public static readonly string ModelName = "ModelName";
        public static readonly string SerialNumber = "SerialNumber";
        public static readonly string DeviceAddressBinding = "DeviceAddressBinding";
        public static readonly string DeviceType = "DeviceType";


    }
}
