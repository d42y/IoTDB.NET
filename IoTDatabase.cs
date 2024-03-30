using IoTDBdotNET.FileDB;
using IoTDBdotNET.TableDB;
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
        private string _flPath;
        public ITimeSeriesDatabase TimeSeries { get; }

        private ConcurrentDictionary<string, dynamic> _tables = new();
        internal ConcurrentDictionary<string, TableInfo> _tableInfos = new();

        private ConcurrentDictionary<string, IFileCollection> _files = new();

        public IoTDatabase(string dbName, string dbPath)
        {
            // Directory checks and creation
            InitializeDirectories(dbName, dbPath);
            if (!Directory.Exists(_dbPath)) throw new DirectoryNotFoundException($"Unable to create database directory. {_dbPath}");
            if (!Directory.Exists(_tsPath)) throw new DirectoryNotFoundException($"Unable to create timeseries directory. {_tsPath}");
            if (!Directory.Exists(_tbPath)) throw new DirectoryNotFoundException($"Unable to create tables directory. {_tbPath}");
            if (!Directory.Exists(_flPath)) throw new DirectoryNotFoundException($"Unable to create files directory. {_flPath}");
            TimeSeries = new TimeSeriesDatabase(_tsPath);
            TimeSeries.ExceptionOccurred += OnExceptionOccurred;
            
            
        }

        #region Tables
        public ITableCollection<T> Tables<T>() where T : class
        {
            string name = typeof(T).Name;
            if (!_tables.ContainsKey(name))
            {
                _tables[name] = new TableCollection<T>(_tbPath, this);
                ((TableCollection<T>)_tables[name]).ExceptionOccurred += OnExceptionOccurred;
                //TableCollection<T> table = _tables[name];

            }
            
            return (ITableCollection<T>)_tables[name];
        }

        internal ITableCollection? GetTable(string name)
        {
            if (_tables.ContainsKey(name))
            {
                return (ITableCollection)_tables[name];
                

            }
            return null;
        }

        #endregion
        public IFileCollection Files(string containerName)
        {
            string name = containerName;
            if (!_files.ContainsKey(name))
            {
                _files[name] = new FileCollection(_flPath, name);
                _files[name].ExceptionOccurred += OnExceptionOccurred;
            }

            return _files[name];
        }

        #region Files


        #endregion

        #region Base Functions
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
            _flPath = Path.Combine(_dbPath, "Files");
            Helper.MachineInfo.CreateDirectory(_flPath);
        }

        

        protected virtual void OnExceptionOccurred(object? sender, ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(sender ?? this, e);
        }

        #endregion

    }
}
