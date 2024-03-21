using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    internal class FileCollection<T> : BaseDatabase where T : FileClass
    {
        public FileCollection(string dbPath, string dbName, double backgroundTaskFromMilliseconds = 100) : base(dbPath, dbName, backgroundTaskFromMilliseconds)
        {
        }

        protected override void InitializeDatabase()
        {
            throw new NotImplementedException();
        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
