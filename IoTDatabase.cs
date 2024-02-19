using LiteDB;
using System.Collections.Concurrent;
using System.Reflection.Metadata.Ecma335;
using System.Threading;

namespace IoTDBdotNET
{
    public class IoTDatabase
    {
        // Define the event based on the delegate
        public event EventHandler<ExceptionEventArgs> ExceptionOccurred;

        private readonly string _dbPath;
        public ITimeSeriesDatabase TimeSeries { get; }
        private ConcurrentDictionary<string, dynamic> _tables = new();


        public IoTDatabase(string dbName, string dbPath, bool createPathIfNotExist = false)
        {
            // Directory checks and creation
            InitializeDirectories(dbPath, createPathIfNotExist);

            var dbPathName = Path.Combine(dbPath, dbName);
            _dbPath = dbPathName;
            InitializeDbPathName(dbPathName);

            // Initialize entity database
            TimeSeries = new TimeSeriesDatabase(dbPathName);
            TimeSeries.ExceptionOccurred += OnExceptionOccurred;

        }

        public ITableCollection<T> Tables<T>(string name) where T : class
        {
            if (!_tables.ContainsKey(name))
            {
                var dbPath = Path.Combine(_dbPath, "Tables");
                InitializeDbPathName(dbPath);
                _tables[name] = new TableCollection<T>(dbPath, name);
                ((TableCollection<T>)_tables[name]).ExceptionOccurred += OnExceptionOccurred;
            }
            return (TableCollection<T>)_tables[name];
        }

        private void InitializeDirectories(string dbPath, bool createPathIfNotExist)
        {
            if (!Directory.Exists(dbPath) && createPathIfNotExist)
            {
                Directory.CreateDirectory(dbPath);
            }
        }

        private void InitializeDbPathName(string dbPathName)
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
