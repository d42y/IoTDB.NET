using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.BlockDB
{
    internal class BlockCollection : BaseDatabase, IBlockCollection
    {

        #region Global Variables
        private readonly string _collectionName = "Collection";
        private bool _processingQueue = false;
        private ConcurrentQueue<BsonValue> _updateEntityQueue = new();
        #endregion

        #region Constructors
        public BlockCollection(string dbPath, string name, string password = "") : base(dbPath, name, password)
        {
            if (!HasIdProperty(typeof(Block)))
            {
                throw new KeyNotFoundException("Table missing Id property with int, long, or Guid data type.");
            }
        }
        #endregion

        #region Base Abstract
        protected override void InitializeDatabase()
        {
            
                var col = Database.GetCollection<Block>(_collectionName);
                // Ensure there is an index on the Timestamp field to make the query efficient
                col.EnsureIndex(x => x.Timestamp);
            
        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region C
        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        public long Count()
        {

            
                return Database.GetCollection<Block>(_collectionName).LongCount();
           

        }
        #endregion
        #region I
        /// <summary>
        /// Insert a new entity to this collection. Document Id must be a new value in collection - Returns document Id
        /// </summary>
        internal BsonValue Insert(BsonValue data)
        {
            var lastBlock = Get();
            if (lastBlock != null && lastBlock.Data == data)
            {
                if (!IsBlockConsistent(2)) throw new InvalidDataException("Block consistency check failed for previous block. Cannot insert new block into existing chain.");
                return lastBlock.Id;
            }
            string previousHash = lastBlock?.Hash ?? "";
            Block newBlock = new Block(previousHash, data);
           
                return Database.GetCollection<Block>(_collectionName).Insert(newBlock);
            

        }
        #endregion

        #region G
        public Block? Get()
        {
           
                var col = Database.GetCollection<Block>(_collectionName);
                // Fetching the last document based on the Timestamp field
                var lastEntry = col.Find(Query.All(Query.Descending), limit: 1).FirstOrDefault();
                return lastEntry;
            
        }

        public List<Block>? Get(int count)
        {
           
                var col = Database.GetCollection<Block>(_collectionName);
                // Fetching the last document based on the Timestamp field
                var lastEntry = col.Find(Query.All(Query.Descending), limit: count).ToList();
                return lastEntry;
            
        }

        public List<Block>? Get(DateTime startDate, DateTime endDate)
        {
            if (startDate.Kind != DateTimeKind.Utc) startDate = startDate.ToUniversalTime();
            if (endDate.Kind != DateTimeKind.Utc) endDate = endDate.ToUniversalTime();
           
                var col = Database.GetCollection<Block>(_collectionName);
                var list = col.Find(Query.Between("Timestamp", startDate, endDate)).ToList();
                return list;
            

        }

        #endregion

        #region V
        public bool IsBlockConsistent(int count)
        {
            var vList = VerifyBlockConsistency(count);
            if (vList == null) return false;
            if (vList.Count == 0) return true;
            var lastItem = vList.LastOrDefault();
            return lastItem.Valid;
        }

        public bool IsBlockConsistent(DateTime startDate, DateTime endDate)
        {
            var vList = VerifyBlockConsistency(startDate, endDate);
            if (vList == null) return false;
            if (vList.Count == 0) return true;
            var lastItem = vList.LastOrDefault();
            return lastItem.Valid;
        }

        public List<(Block Block, bool Valid)> VerifyBlockConsistency(int count)
        {
            var list = Get(count); // Assuming Get fetches blocks between the dates
            List<(Block, bool)> vList = VerifyBlockList(list);
            return vList;
        }

        public List<(Block Block, bool Valid)> VerifyBlockConsistency(DateTime startDate, DateTime endDate)
        {
            var list = Get(startDate, endDate); // Assuming Get fetches blocks between the dates
            List<(Block, bool)> vList = VerifyBlockList(list);
            return vList;
        }

        private List<(Block Block, bool Valid)> VerifyBlockList(List<Block>? list)
        {
            List<(Block, bool)> vList = new();
            string previousHash = string.Empty;

            foreach (var item in list ?? new List<Block>())
            {
                // Assume valid unless proven otherwise
                bool valid = item.Hash == item.CalculateHash() &&
                             (string.IsNullOrEmpty(previousHash) || previousHash == item.PreviousHash);

                if (valid)
                {
                    // Update previousHash only if current block is valid
                    previousHash = item.Hash;
                }
                else
                {
                    break;
                }

                vList.Add((item, valid));
            }
            return vList;
        }
        #endregion
    }
}
