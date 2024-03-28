using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace IoTDBdotNET
{
    internal abstract class BaseDatabase : IDisposable
    {
        public event EventHandler<ExceptionEventArgs>? ExceptionOccurred;
        //private readonly LiteDatabase _db;
        private readonly int _numThreads;
        protected int NumThreads { get { return _numThreads; } }
        private readonly object _syncRoot = new object();
        private CancellationTokenSource _cancellationTokenSource = new();
        private Task? _backgroundTask;
        private readonly double _backgroundTaskFromMilliseconds;
        private readonly string _dbName;
        private readonly string _dbPath;

        private DateTime _lastAccess = DateTime.Now;

        internal readonly string _connectionString;
        public BaseDatabase(string dbPath, string dbName, double backgroundTaskFromMilliseconds = 100)
        {
            int logicalProcessorCount = Environment.ProcessorCount;
            _numThreads = logicalProcessorCount > 1 ? logicalProcessorCount - 1 : 1;
            _dbName = dbName;
            _dbPath = dbPath;
            if (dbName.ToLower().EndsWith(".db")) _dbName = Path.GetFileNameWithoutExtension(dbName);
            _connectionString = Path.Combine(dbPath, $"{_dbName}.db");
            _liteDatabase = new LiteDatabase(_connectionString);
            
            try
            {
                InitializeDatabase();
            }
            catch (NotImplementedException)
            {
                //do nothing
            }
            catch (Exception ex)
            {
                OnExceptionOccurred(new(ex));
            }

            _backgroundTaskFromMilliseconds = backgroundTaskFromMilliseconds;
            StartBackgroundTask();
            
        }
        internal string DbPath { get { return _dbPath; } }
        internal string DbName { get { return _dbName; } }

        //protected ILiteDatabase Database { get { return new LiteDatabase(Path.Combine(_dbPath, $"{_dbName}.db")); } }
        protected CancellationTokenSource CancellationTokenSource { get { return _cancellationTokenSource; } }
        protected object SyncRoot { get { return _syncRoot; } }


        private readonly LiteDatabase _liteDatabase;
        public LiteDatabase Database
        {
            get
            {
                _lastAccess = DateTime.Now;
                return _liteDatabase;
            }
        }

        // Method to raise the event
        protected virtual void OnExceptionOccurred(ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(this, e);
            
        }

        protected virtual void StartBackgroundTask()
        {
            _backgroundTask = Task.Run(async () =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        PerformBackgroundWork(_cancellationTokenSource.Token);
                        
                    }
                    catch (NotImplementedException)
                    {
                        // If PerformBackgroundWork is not implemented, cancel the background task
                        _cancellationTokenSource.Cancel(); // Cancel the task
                        break; // Exit the loop
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(_backgroundTaskFromMilliseconds), _cancellationTokenSource.Token);
                }
            }, _cancellationTokenSource.Token);
        }

        

        protected abstract void PerformBackgroundWork(CancellationToken cancellationToken);

        protected abstract void InitializeDatabase();

        public static string HashUniqueIdentifiers(params (string PropertyName, BsonValue Value)[] uniqueIdentifiers)
        {
            // Step 1: Concatenate the property names and values into a single string
            StringBuilder stringBuilder = new StringBuilder();
            foreach (var identifier in uniqueIdentifiers)
            {
                stringBuilder.Append(identifier.PropertyName);
                stringBuilder.Append("=");
                stringBuilder.Append(identifier.Value.ToString());
                stringBuilder.Append(";");
            }

            // Step 2: Convert the concatenated string to a byte array
            byte[] byteData = Encoding.UTF8.GetBytes(stringBuilder.ToString());

            // Step 3: Use a hash algorithm to generate a hash from the byte array
            using (SHA256 sha256Hash = SHA256.Create())
            {
                byte[] hashData = sha256Hash.ComputeHash(byteData);

                // Step 4: Convert the byte array hash to a hexadecimal string
                StringBuilder hashStringBuilder = new StringBuilder();
                for (int i = 0; i < hashData.Length; i++)
                {
                    hashStringBuilder.Append(hashData[i].ToString("x2")); // "x2" for lowercase hex format
                }

                return hashStringBuilder.ToString();
            }
        }

        protected virtual void StopBackgroundTask()
        {
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
                _backgroundTask?.Wait();
                _cancellationTokenSource.Dispose();
                _backgroundTask = null;
            }
        }

        public void Dispose()
        {
            StopBackgroundTask();
            _liteDatabase?.Dispose();
        }

        #region Properties
        internal static bool HasIdProperty(Type collectionClassType)
        {

            PropertyInfo? idProperty = GetIdProperty(collectionClassType);
            return idProperty != null;
            
        }

        internal static PropertyInfo? GetIdProperty(Type collectionClassType)
        {

            PropertyInfo? idProperty = collectionClassType.GetProperty($"Id");

            if (idProperty != null)
            {
                // Property exists, now you can get its type
                Type idType = idProperty.PropertyType;

                // Check if the property type is int or long
                return idType == typeof(int) || idType == typeof(long) || idType == typeof(Guid) ? idProperty : null;
            }

            return null;
        }

        internal static PropertyInfo? GetRefTableIdProperty(Type collectionClassType, Type refTableType)
        {

            PropertyInfo? refTableIdProperty = collectionClassType.GetProperty($"{refTableType.Name}Id");

            if (refTableIdProperty != null)
            {
                // Property exists, now you can get its type
                Type refIdType = refTableIdProperty.PropertyType;

                // Check if the property type is int or long
                return refIdType == typeof(int) || refIdType == typeof(long) ? refTableIdProperty : null;
            }

            return null;
        }

        internal static PropertyInfo? GetRefTableProperty(Type collectionClassType, Type refTableType)
        {
            // Correctly get the type of the collection instance
            PropertyInfo? refTableListProperty = collectionClassType.GetProperty($"{refTableType.Name}Table");

            if (refTableListProperty != null)
            {
                // Property exists, now you can get its type
                Type refListType = refTableListProperty.PropertyType;

                // Check if the property type is ILiteCollection<U>
                if (refListType.IsGenericType &&
                    refListType.GetGenericTypeDefinition() == typeof(List<>) &&
                    refListType.GenericTypeArguments[0] == refTableType)
                {
                    return refTableListProperty;
                }
            }

            return null;
        }

        internal static void SetGlobalIgnore<T>()
        {
            PropertyInfo[] properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    Type refTableType = property.PropertyType.GetGenericArguments()[0];
                    if (GetIdProperty(refTableType) != null && GetRefTableIdProperty(typeof(T), refTableType) != null)
                    {
                        if (property.Name.Equals($"{refTableType.Name}Table"))
                        {
                            IgnoreProperty<T>(property);
                        }

                    }

                }
            }
        }

        internal static void IgnoreProperty<T>(PropertyInfo propertyInfo)
        {
            // Get the PropertyInfo object for the property name

            if (propertyInfo == null) return;

            // Build an expression tree to represent the property access
            var parameter = Expression.Parameter(typeof(T), "x");
            var propertyAccess = Expression.Property(parameter, propertyInfo);
            var lambda = Expression.Lambda<Func<T, object>>(Expression.Convert(propertyAccess, typeof(object)), parameter);

            // Use the expression tree to ignore the property
            BsonMapper.Global.Entity<T>().Ignore(lambda);
        }

        
        #endregion


    }
}
