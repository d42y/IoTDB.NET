using IoTDBdotNET.Base;
using IoTDBdotNET.TableDB;
using System.Linq.Expressions;

namespace IoTDBdotNET
{

    public interface ITableCollection
    {
        BsonAutoId AutoId { get; }
        EntityMapper EntityMapper { get; }
        string Name { get; }

        long Count();
        long Count(BsonExpression predicate);
        
        long Count(Query query);
        long Count(string predicate, BsonDocument parameters);
        long Count(string predicate, params BsonValue[] args);
        bool Delete(BsonValue id);
        int DeleteAll();
        int DeleteMany(BsonExpression predicate);
        
        int DeleteMany(string predicate, BsonDocument parameters);
        int DeleteMany(string predicate, params BsonValue[] args);
        bool DropIndex(string name);
        bool EnsureIndex(BsonExpression expression, bool unique = false);
        bool EnsureIndex(string name, BsonExpression expression, bool unique = false);
        
        bool Exists(BsonExpression predicate);
        bool Exists(Query query);
        bool Exists(string predicate, BsonDocument parameters);
        bool Exists(string predicate, params BsonValue[] args);
        List<BsonDocument> Find(string columnName, string value, Comparison comparisonType = Comparison.Equals);
        List<BsonDocument> FindAll(int take = 1000, TakeOrder takeOrder = TakeOrder.Last);
        BsonValue Max();
        BsonValue Max(BsonExpression keySelector);
        BsonValue Min();
        BsonValue Min(BsonExpression keySelector);
        long SetAll(string columnName, BsonValue? value);
        int UpdateMany(BsonExpression transform, BsonExpression predicate);
    }

    public interface ITableCollection<T> : ITableCollection where T : class
    {
        long Count(Expression<Func<T, bool>> predicate);
        int DeleteMany(Expression<Func<T, bool>> predicate);
        bool EnsureIndex<K>(Expression<Func<T, K>> keySelector, bool unique = false);
        bool EnsureIndex<K>(string name, Expression<Func<T, K>> keySelector, bool unique = false);
        bool Exists(Expression<Func<T, bool>> predicate);
        
        List<T> Find(BsonExpression predicate, int skip = 0, int limit = int.MaxValue);
        List<T> Find(Expression<Func<T, bool>> predicate, int skip = 0, int limit = int.MaxValue);
        List<T> Find(Query query, int skip = 0, int limit = int.MaxValue);
        List<T> FindAll();
        T FindById(BsonValue id);
        T FindOne(BsonExpression predicate);
        T FindOne(BsonExpression predicate, params BsonValue[] args);
        T FindOne(Expression<Func<T, bool>> predicate);
        T FindOne(Query query);
        T FindOne(string predicate, BsonDocument parameters);
        void Insert(BsonValue id, T entity);
        int Insert(IEnumerable<T> entities);
        BsonValue Insert(T entity);
        int InsertBulk(IEnumerable<T> entities, int batchSize = 5000);
        
        K Max<K>(Expression<Func<T, K>> keySelector);
        
        K Min<K>(Expression<Func<T, K>> keySelector);
        //ILiteQueryable<T> Query();
        QueryBuilder<T> Query();

        bool Update(BsonValue id, T entity);
        int Update(IEnumerable<T> entities);
        void UpdateQueue(T entity);
        bool Update(T entity);
        
        int UpdateMany(Expression<Func<T, T>> extend, Expression<Func<T, bool>> predicate);
        bool Upsert(BsonValue id, T entity);
        int Upsert(IEnumerable<T> entities);
        bool Upsert(T entity);
    }
}