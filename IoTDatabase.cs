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

        public ITableCollection<T> Tables<T>() where T : class
        {
            string name = typeof(T).Name;
            if (!_tables.ContainsKey(name))
            {
                Helper.MachineInfo.CreateDirectory(_tbPath);
                _tables[name] = new TableCollection<T>(_tbPath, this);
                ((TableCollection<T>)_tables[name]).ExceptionOccurred += OnExceptionOccurred;
            }
            return (TableCollection<T>)_tables[name];
        }

        private void InitializeDirectories(string dbName, string dbPath)
        {
            Helper.MachineInfo.CreateDirectory(dbPath);
             var dbPathName = Path.Combine(dbPath, dbName);
            _dbPath = dbPathName;
            Helper.MachineInfo.CreateDirectory(_dbPath);
            _tsPath = Path.Combine(_dbPath, "TimeSeries");
            Helper.MachineInfo.CreateDirectory(_tsPath);
            _tbPath = Path.Combine(_dbPath, "Tables");
            Helper.MachineInfo.CreateDirectory(_tbPath);
        }

        

        protected virtual void OnExceptionOccurred(object? sender, ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(sender ?? this, e);
        }


    }
}
