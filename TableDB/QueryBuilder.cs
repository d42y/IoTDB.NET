using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Xml.Linq;
using IoTDBdotNET;
using IoTDBdotNET.Base;

namespace IoTDBdotNET.TableDB
{
    public class QueryBuilder<T> where T : class
    {

        private IoTDatabase _database;
        private List<Expression<Func<T, bool>>> _filters = new();
        private List<Type> _includes = new();


        public QueryBuilder(IoTDatabase database)
        {
            _database = database;
        }

        public QueryBuilder<T> Find(Expression<Func<T, bool>> filter)
        {
            _filters.Add(filter);
            return this;
        }


        public QueryBuilder<T> Find(string propertyName, object value, Comparison comparisonType)
        {
            var parameterExp = Expression.Parameter(typeof(T), "type");
            var propertyExp = Expression.Property(parameterExp, propertyName);
            Expression? condition = null;

            switch (comparisonType)
            {
                case Comparison.Equals:
                    condition = Expression.Equal(propertyExp, Expression.Constant(value));
                    break;
                case Comparison.StartsWith:
                    condition = Expression.Call(propertyExp, typeof(string).GetMethod(nameof(String.StartsWith), new[] { typeof(string) })!, Expression.Constant(value));
                    break;
                case Comparison.EndsWith:
                    condition = Expression.Call(propertyExp, typeof(string).GetMethod(nameof(String.EndsWith), new[] { typeof(string) })!, Expression.Constant(value));
                    break;
                case Comparison.Contains:
                    condition = Expression.Call(propertyExp, typeof(string).GetMethod(nameof(String.Contains), new[] { typeof(string) })!, Expression.Constant(value));
                    break;
            }

            if (condition != null)
            {
                var lambda = Expression.Lambda<Func<T, bool>>(condition, parameterExp);
                _filters.Add(lambda);
            }

            return this;
        }

        public QueryBuilder<T> Include<K>() where K : class
        {

            _includes.Add(typeof(K));

            return this;
        }

        public List<T> Execute()
        {
            var col = _database.Tables<T>();
            var query = col.FindAll().AsQueryable();

            foreach (var filter in _filters)
            {
                query = query.Where(filter);
            }

            var results = query.ToList();

            // _database.Tables<T>() //get T table
            MethodInfo? method = _database.GetType().GetMethod("Tables");
            if (method == null) return results;

            foreach (var result in results)
            {
                foreach (var include in _includes)
                {

                    MethodInfo genericMethod = method.MakeGenericMethod(include);
                    dynamic? relatedTable = genericMethod.Invoke(_database, null);
                    if (relatedTable == null) continue;
                    var resultId = result.GetType().GetProperty("Id")?.GetValue(result, null);
                    if (resultId == null) continue;
                    var find = Query.EQ($"{typeof(T).Name}Id", new BsonValue(resultId));

                    dynamic relatedEntities = relatedTable.Find(find);
                    string propertyName = $"{result.GetType().Name}Table";

                    // Find the property on the result object
                    PropertyInfo? propertyInfo = result.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
                    if (propertyInfo == null) continue;
                    if (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                    {
                        // Check if the property is already initialized, if not, initialize it
                        var propertyValue = propertyInfo.GetValue(result);
                        if (propertyValue == null)
                        {
                            Type listType = propertyInfo.PropertyType.GetGenericArguments()[0];
                            var newList = Activator.CreateInstance(typeof(List<>).MakeGenericType(listType));
                            propertyInfo.SetValue(result, newList);
                            propertyValue = newList;
                        }

                        if (propertyValue == null) continue;
                        // Assuming relatedEntities is IEnumerable, but need to cast it to the correct type
                        foreach (var entity in relatedEntities)
                        {
                            // Add each related entity to the property list
                            // This requires casting propertyValue back to dynamic to invoke Add
                            ((dynamic)propertyValue).Add(entity);
                        }
                    }


                }
            }

            return results.ToList();
        }

    }
}
