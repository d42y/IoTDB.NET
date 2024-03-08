using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.BlockDB
{
    internal class BlockCollection<T> : BaseDatabase where T : class
    {

        #region Global Variables
        private readonly string _collectionName = "Collection";
        private bool _processingQueue = false;
        private ConcurrentQueue<T> _updateEntityQueue = new ConcurrentQueue<T>();
        private IoTDatabase _database;
        #endregion

        #region Constructors
        public BlockCollection(string dbPath, IoTDatabase database) : base(dbPath, typeof(T).Name)
        {
            if (!HasIdProperty(typeof(T)))
            {
                throw new KeyNotFoundException("Table missing Id property with int, long, or Guid data type.");
            }
            SetGlobalIgnore<T>();
            _database = database;

        }
        #endregion

        #region Base Abstract
        protected override void InitializeDatabase()
        {
            throw new NotImplementedException();
        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
