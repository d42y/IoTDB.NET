using IoTDBdotNET;
using IoTDBdotNET.BlockDB;
using IoTDBdotNET.BlockDB.Attributes;
using IoTDBdotNET.Helper;
using IoTDBdotNET.TableDB;
using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace IoTDBdotNET
{
    internal class TableCollection<T> : BaseDatabase, ITableCollection<T> where T : class
    {
        #region Global Variables
        private readonly string _collectionName = "Collection";
        private bool _processingQueue = false;
        private ConcurrentQueue<T> _updateEntityQueue = new ConcurrentQueue<T>();
        private IoTDatabase _database;
        private List<BlockInfo> _blocksInfo = new();
        private ConcurrentDictionary<string, BlockCollection> _blocks = new();
        #endregion

        #region Constructors
        public TableCollection(string dbPath, IoTDatabase database) : base(dbPath, typeof(T).Name)
        {
            if (!HasIdProperty(typeof(T)))
            {
                throw new KeyNotFoundException("Table missing Id property with int, long, or Guid data type.");
            }
            SetGlobalIgnore<T>();
            var blockChainProperties = ReflectionHelper.GetPropertiesWithBlockChainValueAttribute(typeof(T)).ToList();
            foreach (var prop in blockChainProperties)
            {
                var attribute = prop.GetCustomAttribute<BlockChainValueAttribute>();
                _blocksInfo.Add(new(prop.Name, typeof(BlockChainValueAttribute).Name, attribute?.Description??"", prop));
            }
            _database = database;

        }

        #endregion

        #region A
        /// <summary>
        /// Get collection auto id type
        /// </summary>
        public BsonAutoId AutoId
        {
            get
            {
                using (var db = new LiteDatabase(ConnectionString))
                {
                    return db.GetCollection<T>(_collectionName).AutoId;
                }
            }
        }
        #endregion

        #region C
        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        public long Count()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount();
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public long Count(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public long Count(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate, parameters);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public long Count(string predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate, args);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long Count(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long Count(Query query)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(query);
            }

        }

        #endregion

        #region BlockChain
        public List<BlockInfo> BlocksInfo
        {
            get
            {
                return _blocksInfo;
            }
        }

        public BlockInfo TagPropertyAsBlockInfo(string name, string description = "")
        {
            var prop = ReflectionHelper.GetProperty(typeof(T), name);
            if (prop == null) throw new KeyNotFoundException(name);
            BlockInfo bi = new(name, "Manual Tag BlockChainValueAttribute", description, prop);
            _blocksInfo.Add(bi);
            return bi;
        }

        public IBlockCollection? Blocks(string name)
        {
            if (!_blocksInfo.Any(x => x.Name == name))
            {
                throw new EntryPointNotFoundException($"{typeof(T).Name} does not have BlockChainValueAttribute with name {name}.");
            }

            if (!_blocks.ContainsKey(name))
            {
                var blockPath = Path.Combine(DbPath, "BlockChain");
                Helper.MachineInfo.CreateDirectory(blockPath);
                _blocks[name] = new BlockCollection(blockPath, $"{DbName}_{name}");
                _blocks[name].ExceptionOccurred += OnBlockExceptionOccurred;
            }
            return _blocks[name];
        }

        private void WriteToBlocks (T entity)
        {
            foreach(var bi in _blocksInfo)
            {
                var value = bi.Property.GetValue(entity);
                if (value == null) continue;
                _blocks[bi.Name].Insert(new BsonValue(value));
            }
            
        }

        #endregion

        #region D
        /// <summary>
        /// Drop index and release slot for another index
        /// </summary>
        public bool DropIndex(string name)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DropIndex(name);
            }

        }

        /// <summary>
        /// Delete a single document on collection based on _id index. Returns true if document was deleted
        /// </summary>
        public bool Delete(BsonValue id)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Delete(id);
            }

        }
        /// <summary>
        /// Delete all documents inside collection. Returns how many documents was deleted. Run inside current transaction
        /// </summary>
        public int DeleteAll()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DeleteAll();
            }

        }
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        public int DeleteMany(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DeleteMany(predicate);
            }

        }
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        public int DeleteMany(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DeleteMany(predicate, parameters);
            }

        }
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        public int DeleteMany(string predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DeleteMany(predicate, args);
            }

        }
        /// <summary>
        /// Delete all documents based on predicate expression. Returns how many documents was deleted
        /// </summary>
        public int DeleteMany(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).DeleteMany(predicate);
            }

        }
        #endregion

        #region E

        /// <summary>
        /// Getting entity mapper from current collection. Returns null if collection are BsonDocument type
        /// </summary>
        public EntityMapper EntityMapper
        {
            get
            {
                using (var db = new LiteDatabase(ConnectionString))
                {
                    return db.GetCollection<T>(_collectionName).EntityMapper;
                }
            }
        }

        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already. Returns true if index was created or false if already exits
        /// </summary>
        /// <param name="name">Index name - unique name for this collection</param>
        /// <param name="expression">Create a custom expression function to be indexed</param>
        /// <param name="unique">If is a unique index</param>
        public bool EnsureIndex(string name, BsonExpression expression, bool unique = false)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).EnsureIndex(name, expression, unique);
            }

        }
        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already. Returns true if index was created or false if already exits
        /// </summary>
        /// <param name="expression">Document field/expression</param>
        /// <param name="unique">If is a unique index</param>
        public bool EnsureIndex(BsonExpression expression, bool unique = false)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).EnsureIndex(expression, unique);
            }

        }

        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already.
        /// </summary>
        /// <param name="name">Index name - unique name for this collection</param>
        /// <param name="keySelector">LinqExpression to be converted into BsonExpression to be indexed</param>
        /// <param name="unique">Create a unique keys index?</param>
        public bool EnsureIndex<K>(string name, Expression<Func<T, K>> keySelector, bool unique = false)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).EnsureIndex(name, keySelector, unique);
            }

        }

        /// <summary>
        /// Create a new permanent index in all documents inside this collections if index not exists already.
        /// </summary>
        /// <param name="keySelector">LinqExpression to be converted into BsonExpression to be indexed</param>
        /// <param name="unique">Create a unique keys index?</param>
        public bool EnsureIndex<K>(Expression<Func<T, K>> keySelector, bool unique = false)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).EnsureIndex(keySelector, unique);
            }

        }

        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public bool Exists(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Exists(predicate);
            }

        }
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public bool Exists(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Exists(predicate, parameters);
            }

        }
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public bool Exists(string predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Exists(predicate, args);
            }

        }
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public bool Exists(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Exists(predicate);
            }

        }
        /// <summary>
        /// Returns true if query returns any document. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public bool Exists(Query query)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Exists(query);
            }

        }

        #endregion

        #region F
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        public List<T> Find(BsonExpression predicate, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(predicate, skip, limit).ToList();
            }

        }


        /// <summary>
        /// Find documents inside a collection using query definition.
        /// </summary>
        public List<T> Find(Query query, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(query, skip, limit).ToList();
            }

        }
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        public List<T> Find(Expression<Func<T, bool>> predicate, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(predicate, skip, limit).ToList();
            }

        }
        /// <summary>
        /// Find a document using Document Id. Returns null if not found.
        /// </summary>
        public T FindById(BsonValue id)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindById(id);
            }

        }
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        public T FindOne(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindOne(predicate);
            }

        }
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        public T FindOne(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindOne(predicate, parameters);
            }

        }
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        public T FindOne(BsonExpression predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindOne(predicate, args);
            }

        }
        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        public T FindOne(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindOne(predicate);
            }

        }
        /// <summary>
        /// Find the first document using defined query structure. Returns null if not found
        /// </summary>
        public T FindOne(Query query)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindOne(query);
            }

        }
        /// <summary>
        /// Returns all documents inside collection order by _id index.
        /// </summary>
        public List<T> FindAll()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindAll().ToList();
            }

        }
        #endregion

        #region I
        /// <summary>
        /// Insert a new entity to this collection. Document Id must be a new value in collection - Returns document Id
        /// </summary>
        public BsonValue Insert(T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                return db.GetCollection<T>(_collectionName).Insert(entity);
            }

        }

        /// <summary>
        /// Insert a new document to this collection using passed id value.
        /// </summary>
        public void Insert(BsonValue id, T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                db.GetCollection<T>(_collectionName).Insert(id, entity);
            }

        }

        /// <summary>
        /// Insert an array of new documents to this collection. Document Id must be a new value in collection. Can be set buffer size to commit at each N documents
        /// </summary>
        public int Insert(IEnumerable<T> entities)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                foreach (var entity in entities)
                {
                    WriteToBlocks(entity);
                }
                return db.GetCollection<T>(_collectionName).Insert(entities);
            }

        }

        /// <summary>
        /// Implements bulk insert documents in a collection. Usefull when need lots of documents.
        /// </summary>
        public int InsertBulk(IEnumerable<T> entities, int batchSize = 5000)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                foreach (var entity in entities)
                {
                    WriteToBlocks(entity);
                }
                return db.GetCollection<T>(_collectionName).InsertBulk(entities, batchSize);
            }

        }
        #endregion

        #region M
        /// <summary>
        /// Returns the min value from specified key value in collection
        /// </summary>
        public BsonValue Min(BsonExpression keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Min(keySelector);
            }

        }
        /// <summary>
        /// Returns the min value of _id index
        /// </summary>
        public BsonValue Min()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Min();
            }

        }
        /// <summary>
        /// Returns the min value from specified key value in collection
        /// </summary>
        public K Min<K>(Expression<Func<T, K>> keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Min(keySelector);
            }

        }
        /// <summary>
        /// Returns the max value from specified key value in collection
        /// </summary>
        public BsonValue Max(BsonExpression keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Max(keySelector);
            }

        }
        /// <summary>
        /// Returns the max _id index key value
        /// </summary>
        public BsonValue Max()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Max();
            }

        }
        /// <summary>
        /// Returns the last/max field using a linq expression
        /// </summary>
        public K Max<K>(Expression<Func<T, K>> keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Max(keySelector);
            }

        }
        #endregion

        #region N
        /// <summary>
        /// Get collection name
        /// </summary>
        public string Name => _collectionName;
        #endregion

        #region O
        protected void OnBlockExceptionOccurred(object? sender, ExceptionEventArgs e)
        {
            OnExceptionOccurred(e);
        }
        #endregion

        #region Q
        public QueryBuilder<T> Query()
        {
            return new QueryBuilder<T>(_database);
        }

        #endregion

        #region U
        /// <summary>
        /// Insert or Update a document in this collection.
        /// </summary>
        public bool Upsert(T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                return db.GetCollection<T>(_collectionName).Upsert(entity);
            }

        }
        /// <summary>
        /// Insert or Update all documents
        /// </summary>
        public int Upsert(IEnumerable<T> entities)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                foreach (var entity in entities)
                {
                    WriteToBlocks(entity);
                }
                return db.GetCollection<T>(_collectionName).Upsert(entities);
            }

        }

        /// <summary>
        /// Insert or Update a document in this collection.
        /// </summary>
        public bool Upsert(BsonValue id, T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                return db.GetCollection<T>(_collectionName).Upsert(id, entity);
            }

        }

        /// <summary>
        /// Enque update for background processing. No return.
        /// </summary>
        /// <param name="entity"></param>
        public void UpdateQueue(T entity)
        {
            if (_blocksInfo.Count > 0) { throw new NotSupportedException("UpdateQueue is not supported for T with BlockChainValue attributes"); }
            _updateEntityQueue.Enqueue(entity);

        }
        /// <summary
        /// <summary>
        /// Update a document in this collection. Returns false if not found document in collection
        /// </summary>
        public bool Update(T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                return db.GetCollection<T>(_collectionName).Update(entity);
            }

        }


        /// <summary>
        /// Update a document in this collection. Returns false if not found document in collection
        /// </summary>
        public bool Update(BsonValue id, T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                WriteToBlocks(entity);
                return db.GetCollection<T>(_collectionName).Update(id, entity);
            }

        }

        /// <summary>
        /// Update all documents
        /// </summary>
        public int Update(IEnumerable<T> entities)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                foreach (var entity in entities)
                {
                    WriteToBlocks(entity);
                }
                return db.GetCollection<T>(_collectionName).Update(entities);
            }

        }

        /// <summary>
        /// Update many documents based on transform expression. This expression must return a new document that will be replaced over current document (according with predicate).
        /// Eg: col.UpdateMany("{ Name: UPPER($.Name), Age }", "_id > 0")
        /// </summary>
        public int UpdateMany(BsonExpression transform, BsonExpression predicate)
        {
            if (_blocksInfo.Count > 0) { throw new NotSupportedException("UpdateMany is not supported for T with BlockChainValue attributes"); }
            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).UpdateMany(transform, predicate);
            }

        }

        /// <summary>
        /// Update many document based on merge current document with extend expression. Use your class with initializers. 
        /// Eg: col.UpdateMany(x => new Customer { Name = x.Name.ToUpper(), Salary: 100 }, x => x.Name == "John")
        /// </summary>
        public int UpdateMany(Expression<Func<T, T>> extend, Expression<Func<T, bool>> predicate)
        {
            if (_blocksInfo.Count > 0) { throw new NotSupportedException("UpdateMany is not supported for T with BlockChainValue attributes"); }
            using (var db = new LiteDatabase(ConnectionString))
            {
                
                return db.GetCollection<T>(_collectionName).UpdateMany(extend, predicate);
            }

        }

        #endregion

        #region Base Functions

        protected override void InitializeDatabase()
        {

        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            if (!_updateEntityQueue.IsEmpty && !_processingQueue)
            {
                lock (SyncRoot)
                {
                    _processingQueue = true;
                    try
                    {
                        CommitUpdate();
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                    }
                    finally
                    {
                        _processingQueue = false;
                    }
                }
            }
        }

        private void CommitUpdate()
        {
            int count = 0;
            Dictionary<long, T> entityList = new();
            List<long> id = new List<long>();
            while (_updateEntityQueue.TryDequeue(out var entity) && count++ <= 1000)
            {
                var idProperty = GetIdProperty(typeof(T));
                if (idProperty == null)
                {
                    OnExceptionOccurred(new(new InvalidOperationException("Type T must have a property named 'Id'.")));
                }
                else
                {
                    long entityId = Convert.ToInt64(idProperty.GetValue(entity));
                    if (!entityList.ContainsKey(entityId))
                    {
                        entityList.Add(entityId, entity);
                    }
                    else
                    {
                        entityList[entityId] = entity;
                    }
                }
            }
            if (entityList.Count > 0)
            {
                using (var db = new LiteDatabase(ConnectionString))
                {
                    var entities = db.GetCollection<T>(_collectionName);
                    entities.Update(entityList.Select(x => x.Value).ToList());
                }
            }
        }

        #endregion base functions
    }
}
