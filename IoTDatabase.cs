using System.Collections.Concurrent;

namespace IoTDBdotNET
{
    public class IoTDatabase
    {
        // Define the event based on the delegate
        public event EventHandler<ExceptionEventArgs>? ExceptionOccurred;

        private string _dbPath;
        private string _tsPath;
        private string _tbPath;
        public ITimeSeriesDatabase TimeSeries { get; }
        private ConcurrentDictionary<string, dynamic> _tables = new();


        public IoTDatabase(string dbName, string dbPath)
        {
            // Directory checks and creation
            InitializeDirectories(dbName, dbPath);
            if (!Directory.Exists(_dbPath)) throw new DirectoryNotFoundException($"Unable to create database directory. {_dbPath}");
            if (!Directory.Exists(_tsPath)) throw new DirectoryNotFoundException($"Unable to create timeseries directory. {_tsPath}");
            if (!Directory.Exists(_tbPath)) throw new DirectoryNotFoundException($"Unable to create tables directory. {_tbPath}");
            TimeSeries = new TimeSeriesDatabase(_tsPath);
            TimeSeries.ExceptionOccurred += OnExceptionOccurred;

        }

        private ILiteCollection<BsonDocument>? GetBsonTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            var dbTablePath = Path.Combine(_tbPath, $"{tableName}.db");
            if (!File.Exists(dbTablePath)) return null;
            // Open database
            using (var db = new LiteDatabase(@"MyData.db"))
            {
                // Get a collection (or create, if doesn't exist)
                var col = db.GetCollection("Collection");
                return col; 
            }
           
        }

        public ITableCollection<T> Tables<T>() where T : class
        {
            string name = typeof(T).Name;
            if (!_tables.ContainsKey(name))
            {
                CreateDirectory(_tbPath);
                _tables[name] = new TableCollection<T>(_tbPath, this);
                ((TableCollection<T>)_tables[name]).ExceptionOccurred += OnExceptionOccurred;
            }
            return (TableCollection<T>)_tables[name];
        }



        private void InitializeDirectories(string dbName, string dbPath)
        {
            CreateDirectory(dbPath);
             var dbPathName = Path.Combine(dbPath, dbName);
            _dbPath = dbPathName;
            CreateDirectory(_dbPath);
            _tsPath = Path.Combine(_dbPath, "TimeSeries");
            CreateDirectory(_tsPath);
            _tbPath = Path.Combine(_dbPath, "Tables");
            CreateDirectory(_tbPath);
        }

        private void CreateDirectory(string dbPathName)
        {
            if (!Directory.Exists(dbPathName))
            {
                Directory.CreateDirectory(dbPathName);
            }
        }

        protected virtual void OnExceptionOccurred(object? sender, ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(sender ?? this, e);
        }


    }
}
