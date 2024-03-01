using IoTDBdotNET;
using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml;
using System.Xml.Linq;

namespace IoTDBdotNET
{
    internal class TableCollection<T> : BaseDatabase, ITableCollection<T>
    {
        private readonly string _collectionName = "table";
        private bool _processingQueue = false;
        private ConcurrentQueue<T> _updateEntityQueue = new ConcurrentQueue<T>();

        public TableCollection(string dbPath, string tableName) : base(dbPath, tableName)
        {
            if (!HasIdProperty())
            {
                throw new KeyNotFoundException("Table missing Id property with int or long data type.");
            }
        }

        #region ILiteCollection
        /// <summary>
        /// Get collection name
        /// </summary>
        public string Name => _collectionName;

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
        /// Run an include action in each document returned by Find(), FindById(), FindOne() and All() methods to load DbRef documents
        /// Returns a new Collection with this action included
        /// </summary>
        public ILiteCollection<T> Include<K>(Expression<Func<T, K>> keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Include<K>(keySelector);
            }

        }
        /// <summary>
        /// Run an include action in each document returned by Find(), FindById(), FindOne() and All() methods to load DbRef documents
        /// Returns a new Collection with this action included
        /// </summary>
        public ILiteCollection<T> Include(BsonExpression keySelector)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Include(keySelector);
            }

        }
        /// <summary>
        /// Insert or Update a document in this collection.
        /// </summary>
        public bool Upsert(T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
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
                return db.GetCollection<T>(_collectionName).Upsert(id, entity);
            }

        }

        /// <summary>
        /// Enque update for background processing. No return.
        /// </summary>
        /// <param name="entity"></param>
        public void UpdateQueue(T entity)
        {

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
                return db.GetCollection<T>(_collectionName).Update(entities);
            }

        }

        /// <summary>
        /// Update many documents based on transform expression. This expression must return a new document that will be replaced over current document (according with predicate).
        /// Eg: col.UpdateMany("{ Name: UPPER($.Name), Age }", "_id > 0")
        /// </summary>
        public int UpdateMany(BsonExpression transform, BsonExpression predicate)
        {

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

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).UpdateMany(extend, predicate);
            }

        }

        /// <summary>
        /// Insert a new entity to this collection. Document Id must be a new value in collection - Returns document Id
        /// </summary>
        public BsonValue Insert(T entity)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
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
                return db.GetCollection<T>(_collectionName).InsertBulk(entities, batchSize);
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
        /// Return a new LiteQueryable to build more complex queries
        /// </summary>
        public ILiteQueryable<T> Query()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Query();
            }

        }
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        public IEnumerable<T> Find(BsonExpression predicate, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(predicate, skip, limit);
            }

        }
        /// <summary>
        /// Find documents inside a collection using query definition.
        /// </summary>
        public IEnumerable<T> Find(Query query, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(query, skip, limit);
            }

        }
        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        public IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip = 0, int limit = int.MaxValue)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Find(predicate, skip, limit);
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
        public IEnumerable<T> FindAll()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).FindAll();
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
        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        public int Count()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count();
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public int Count(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public int Count(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count(predicate, parameters);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any document. Needs indexes on query expression
        /// </summary>
        public int Count(string predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count(predicate, args);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public int Count(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public int Count(Query query)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).Count(query);
            }

        }
        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        public long LongCount()
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount();
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long LongCount(BsonExpression predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long LongCount(string predicate, BsonDocument parameters)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate, parameters);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long LongCount(string predicate, params BsonValue[] args)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate, args);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long LongCount(Expression<Func<T, bool>> predicate)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(predicate);
            }

        }
        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        public long LongCount(Query query)
        {

            using (var db = new LiteDatabase(ConnectionString))
            {
                return db.GetCollection<T>(_collectionName).LongCount(query);
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
        #endregion ILiteCollection

        #region base functions

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
                var idProperty = typeof(T).GetProperty("Id");
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


        private static bool HasIdProperty()
        {

            PropertyInfo? idProperty = typeof(T).GetProperty("Id");

            if (idProperty != null)
            {
                // Property exists, now you can get its type
                Type idType = idProperty.PropertyType;

                // Check if the property type is int or long
                return idType == typeof(int) || idType == typeof(long);
            }

            return false;
        }
        #endregion base functions
    }
}
