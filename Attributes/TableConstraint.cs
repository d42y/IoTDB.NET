using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.Attributes
{
    public enum TableConstraint
    {
        Cascading,
        Restrictive,
        NoAction,
        SetNull,
        SetDefault
    }
}
