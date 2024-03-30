using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public enum FileOperation
    {
        AbandonCheckout,
        CheckIn,
        CheckOut,
        ClearCheckout,
        Delete,
        Get,
        New,
        Rename
    }
}
