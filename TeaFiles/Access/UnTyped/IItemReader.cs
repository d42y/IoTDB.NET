// copyright discretelogics 2012. released under the gpl v3. see license.txt for details.
namespace IoTDBdotNET
{
    interface IItemReader
    {
        long Count { get; }
        bool CanRead { get; }
        Item Read();
        void SetFilePointerToItem(int i);
    }
}
