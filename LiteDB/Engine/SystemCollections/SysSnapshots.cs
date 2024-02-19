using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static IoTDBdotNET.Constants;

namespace IoTDBdotNET.Engine
{
    public partial class LiteEngine
    {
        private IEnumerable<BsonDocument> SysSnapshots()
        {
            foreach (var transaction in _monitor.Transactions)
            {
                foreach (var snapshot in transaction.Snapshots)
                {
                    yield return new BsonDocument
                    {
                        ["transactionID"] = (int)transaction.TransactionID,
                        ["collection"] = snapshot.CollectionName,
                        ["mode"] = snapshot.Mode.ToString(),
                        ["readVersion"] = snapshot.ReadVersion,
                        ["pagesInMemory"] = snapshot.LocalPages.Count,
                        ["collectionDirty"] = snapshot.CollectionPage?.IsDirty ?? false
                    };
                }
            }
        }
    }
}