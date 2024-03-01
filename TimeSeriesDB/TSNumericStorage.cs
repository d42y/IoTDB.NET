using System.Collections.Concurrent;
using System.IO;
using System.Timers;
using System.Xml.Linq;

namespace IoTDBdotNET
{
    internal class TSNumericStorage : IDisposable
    {
        // Define the event based on the delegate
        public event EventHandler<ExceptionEventArgs>? ExceptionOccurred;

        
        private Dictionary<string, TeaFile<TeaItem>> _TeaFiles = new();

        private string _name;
        private string basePath;
        private ConcurrentQueue<(long id, double value, DateTime timestamp)> _queue;
        private CancellationTokenSource cancellationTokenSource;
        private Task? writeTask;
        private bool _queueProcessing = false;
        private readonly object _syncRoot = new object();
        private readonly int _maxItemsPerFlush;
        private System.Timers.Timer? dailyTimer;

        public TSNumericStorage(string name, string basePath, bool createDirectoryIfNotExist = false)
        {
            if (!Directory.Exists(basePath))
            {
                if (createDirectoryIfNotExist)
                {
                    try
                    {
                        Directory.CreateDirectory(basePath);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Failed to create directory.", ex);
                    }
                }
                else
                {
                    throw new DirectoryNotFoundException($"Directory not found. {basePath}");
                }
            }
            _name = name;
            _maxItemsPerFlush = Helper.Limits.GetMaxProcessingItems();
            this.basePath = basePath;
            _queue = new ConcurrentQueue<(long id, double value, DateTime timestamp)>();
            cancellationTokenSource = new CancellationTokenSource();
            StartBackgroundTask();
            // Initialize and start the daily timer
            InitializeDailyTask();
        }

        //private TeaFile<TeaItem> OpenFile(string path, bool write = false)
        //{
        //    lock(_fileLockObject)
        //    {
        //        if (write)
        //        {
        //            if (_openWriteTeaFiles.ContainsKey(path))
        //            {
        //                return _openWriteTeaFiles[path];
        //            } else if (_openReadTeaFiles.ContainsKey(path))
        //            {
        //                var tf = _openReadTeaFiles[path];
        //                tf.Dispose();
        //                _openReadTeaFiles.Remove(path);
        //            }

        //            if (File.Exists(path))
        //            {
        //                // If the target file exists, append items to it
        //                var tf = TeaFile<TeaItem>.Append(path);
        //                _openWriteTeaFiles[path] = tf;
        //            }
        //            else
        //            {
        //                // If the target file does not exist, create it and write items
        //                var tf = TeaFile<TeaItem>.Create(path);
        //                _openWriteTeaFiles[path] = tf;
        //            }
        //            return _openWriteTeaFiles[path];
        //        }
        //    }
        //}
        
        private void StartBackgroundTask()
        {
            writeTask = Task.Run(async () =>
            {
                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {

                    try
                    {
                        if (_queue.Count > 0 && !_queueProcessing)
                        {
                            FlushQueue();
                        }
                    }
                    catch (Exception ex)
                    {
                        OnExceptionOccurred(new(ex));
                        lock (_syncRoot) { _queueProcessing = false; }
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationTokenSource.Token);
                }
            }, cancellationTokenSource.Token);
        }

        private void InitializeDailyTask()
        {
            dailyTimer = new System.Timers.Timer(60 * 1000); // Check every minute
            dailyTimer.Elapsed += CheckAndPerformConsolidation;
            dailyTimer.Start();

            // Perform an immediate check in case we missed the schedule
            Task.Run(() => CheckAndPerformConsolidation(this, null));
        }

        private void CheckAndPerformConsolidation(object? sender, ElapsedEventArgs? e)
        {
            // Run at startup or if the current time is just after 1 AM
            if (e == null || DateTime.Now.Hour == 1 && DateTime.Now.Minute < 1)
            {
                ConsolidateFiles();
            }
        }

        private void ConsolidateFiles()
        {
            try
            {
                lock (_syncRoot) // Acquire the lock  
                {
                    string dataPath = Path.Combine(basePath, "data");
                    if (!Directory.Exists(dataPath))
                    {
                        Directory.CreateDirectory(dataPath);
                    }

                    string targetFilePath = Path.Combine(dataPath, $"{_name}.tea");
                    var yesterday = DateTime.UtcNow.Date.AddDays(-1);
                    var oldFiles = Directory.GetFiles(basePath, $"{_name}_*.tea")
                                            .Where(f => DateTime.TryParseExact(Path.GetFileNameWithoutExtension(f).Substring(_name.Length + 1), "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var fileDate) && fileDate < yesterday);

                    foreach (var oldFile in oldFiles)
                    {
                        // Open the old file for reading
                        using (var oldTeaFile = TeaFile<TeaItem>.OpenRead(oldFile))
                        {
                            var items = oldTeaFile.Items.ToList(); // Read all items from the old file

                            // Append items to the target file
                            if (File.Exists(targetFilePath))
                            {
                                // If the target file exists, append items to it
                                using (var targetTeaFile = TeaFile<TeaItem>.Append(targetFilePath))
                                {

                                    targetTeaFile.Write(items);

                                }
                            }
                            else
                            {
                                // If the target file does not exist, create it and write items
                                using (var targetTeaFile = TeaFile<TeaItem>.Create(targetFilePath))
                                {
                                    targetTeaFile.Write(items);
                                }
                            }
                        }

                        // Delete the old file after successful consolidation
                        if (File.Exists(oldFile))
                        {
                            File.Delete(oldFile);
                            var path = Path.GetDirectoryName(oldFile)??"";
                            var name = Path.GetFileNameWithoutExtension(oldFile);
                            var workingFile = Path.Combine(path, $"{name}-log.+tea");
                            var backupFile = Path.Combine(path, $"{name}.-tea");
                            if (File.Exists(workingFile)) File.Delete(workingFile);
                            if (File.Exists(backupFile)) File.Delete(backupFile);
                        }
                    }
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        public void Add(long id, double value, DateTime timestamp = default)
        {
            try
            {
                if (timestamp == default) timestamp = DateTime.UtcNow;
                if (timestamp.Kind != DateTimeKind.Utc) timestamp = timestamp.ToUniversalTime();

                _queue.Enqueue((id, value, timestamp));
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
        }

        private void FlushQueue()
        {
            lock (_syncRoot) // Acquire the lock
            {
                try
                {

                    _queueProcessing = true;
                    //const int MaxItemsPerFlush = 5000; // Adjust this value as needed
                    Dictionary<string, List<TeaItem>> openFiles = new();
                    int itemsProcessed = 0;

                    while (itemsProcessed <= _maxItemsPerFlush && _queue.TryDequeue(out var item))
                    {
                        var filePath = GetFilePathForItem(item.timestamp);
                        if (!openFiles.ContainsKey(filePath))
                        {
                            openFiles.Add(filePath, new List<TeaItem>());
                        }
                        openFiles[filePath].Add(new TeaItem { EntityId = item.id, Timestamp = item.timestamp, Value = item.value });
                        itemsProcessed++;
                    }

                    foreach (var file in openFiles)
                    {
                        ProcessFile(file.Key, file.Value);
                    }

                }
                catch (Exception ex) { OnExceptionOccurred(new(ex)); }

                _queueProcessing = false;
            }
        }

        private void ProcessFile(string filePath, List<TeaItem> itemsToWrite)
        {
            var name = Path.GetFileNameWithoutExtension(filePath);
            var path = Path.GetDirectoryName(filePath)??"";

            var savedFile = Path.Combine(path, $"{name}-log.+tea");
            var backupFile = Path.Combine(path, $"{name}.-tea");

            //create a copy of the main file if no save
            if (File.Exists(filePath) && !File.Exists(backupFile))
            {
                File.Copy(filePath, backupFile, true);
            }

            //begin write
            try
            {
                AppendItemsToFile(filePath, itemsToWrite);
            }
            catch (Exception ex) //write file corrupted
            {
                OnExceptionOccurred(new(ex));
                if (File.Exists(savedFile))
                {
                    
                    try
                    {
                        using (var tf = TeaFile<TeaItem>.OpenRead(savedFile))
                        {
                            AppendItemsToFile(backupFile, tf.Items.ToList());
                        }
                        File.Delete(savedFile);
                        File.Copy(backupFile, filePath, true);
                        AppendItemsToFile(filePath, itemsToWrite);
                    }
                    catch (Exception ex2) //main file corruped
                    {
                        OnExceptionOccurred(new(ex2));
                        if (File.Exists(backupFile))
                        {
                            File.Copy(backupFile, filePath, true); //restore from backup
                            try
                            {
                                AppendItemsToFile(filePath, itemsToWrite);

                            } catch (Exception ex3)
                            {
                                OnExceptionOccurred(new(ex3));
                                ProcessCorruptFile(filePath, itemsToWrite);
                            }
                        }
                        else //no saveFile and no backup
                        {
                            ProcessCorruptFile(filePath, itemsToWrite);
                        }
                    }
                }
                else //no saveFile and no backup
                {
                    ProcessCorruptFile(filePath, itemsToWrite);
                }
            }

            try
            {
                AppendItemsToFile(savedFile, itemsToWrite);
            }
            catch { File.Delete(savedFile); File.Copy(filePath, backupFile, true); }

            //make a backup
            UpdateBackupFile(filePath, savedFile, backupFile, TimeSpan.FromMinutes(15));

        }

        private void AppendItemsToFile(string filePath, List<TeaItem> itemsToWrite)
        {
            if (File.Exists(filePath))
            {
                using (var tf = TeaFile<TeaItem>.Append(filePath))
                {
                    tf.Write(itemsToWrite);
                    if (_queue.Count == 0) tf.Close();
                }
            } else
            {
                using (var tf = TeaFile<TeaItem>.Create(filePath))
                {
                    tf.Write(itemsToWrite);
                    if (_queue.Count == 0) tf.Close();
                }
            }
        }


        private void UpdateBackupFile(string origionalFile, string savedFile, string backupFile, TimeSpan timespan)
        {
            //Replace the backup file with the current state of the original file
            if (File.Exists(savedFile))
            {
                if (File.Exists(backupFile))
                {
                    if (IsFileNewerThan(savedFile, backupFile, timespan))
                    {
                        try
                        {
                            using (var tf = TeaFile<TeaItem>.OpenRead(savedFile))
                            {

                                AppendItemsToFile(backupFile, tf.Items.ToList());

                            }
                        }
                        catch { if (File.Exists(origionalFile)) File.Copy(origionalFile, backupFile, true); }
                        File.Delete(savedFile);
                    }
                }

            }

        }

        private void ProcessCorruptFile(string filePath, List<TeaItem> itemsToWrite)
        {
            var name = Path.GetFileNameWithoutExtension(filePath);
            var path = Path.GetDirectoryName(filePath) ?? "";
            var corruptPath = Path.Combine(path, "Corrupted");
            if (!Directory.Exists(corruptPath)) Directory.CreateDirectory(corruptPath);
            var corruptFile = Path.Combine(corruptPath, $"{name}_{DateTime.Now.ToString("yyyy_MM_dd_mm_ss")}.bad");
            File.Move(filePath, corruptFile, true); //move file

            //create new file and write
            AppendItemsToFile(filePath, itemsToWrite);
        }

        private bool IsFileNewerThan(string filePath1, string filePath2, TimeSpan timeSpan)
        {
            FileInfo file1Info = new FileInfo(filePath1);
            FileInfo file2Info = new FileInfo(filePath2);

            // Ensure both files exist
            if (!file1Info.Exists || !file2Info.Exists)
            {
                throw new FileNotFoundException("One or both of the files do not exist.");
            }

            // Compare last write times
            DateTime lastWriteTime1 = file1Info.LastWriteTime;
            DateTime lastWriteTime2 = file2Info.LastWriteTime;

            // Check if file1 is at least 'timeSpan' newer than file2
            return lastWriteTime1 - lastWriteTime2 >= timeSpan;
        }

        private string GetFilePathForItem(DateTime timestamp)
        {
            try
            {
                return Path.Combine(basePath, $"{_name}_{timestamp:yyyyMMdd}.tea");
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return string.Empty;
        }

        //read

        public List<TSItem> GetData(List<long> ids, DateTime from, DateTime to)
        {
            List<TSItem>? items = null;
            try
            {
                if (from.Kind != DateTimeKind.Utc)
                {
                    from = from.ToUniversalTime();
                }
                if (to.Kind != DateTimeKind.Utc)
                {
                    to = to.ToUniversalTime();
                }
                var files = Directory.GetFiles(basePath, $"{_name}_*.tea");

                foreach (var file in files)
                {
                    var name = Path.GetFileNameWithoutExtension(file);

                    DateTime.TryParseExact(name.Substring(name.LastIndexOf('_') + 1), "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var date);
                    if (date.Date >= from.Date && date.Date <= to.Date)
                    {
                        var filePath = Path.Combine(basePath, $"{name}.tea");
                        items = ReadItemsFromFile(filePath, ids, from, to);
                    }

                }

                var memItems = ReadItemsFromFile(Path.Combine(basePath, "data", $"{_name}.tea"), ids, from, to);
                if (memItems != null)
                {
                    if (items == null) { items = new List<TSItem>(); }
                    items.AddRange(memItems);
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return items ?? new();

        }

        private List<TSItem> ReadItemsFromFile(string filePath, List<long> targetIds, DateTime from, DateTime to)
        {
            var items = new List<TSItem>();
            try
            {
                
                lock (_syncRoot) // Acquire the lock 
                {
                    Time _from = from;
                    Time _to = to;
                    using (var tf = TeaFile<TeaItem>.OpenRead(filePath))
                    {
                        var tsItems = tf.Items.Where(item => targetIds.Contains(item.EntityId) && item.Timestamp >= _from && item.Timestamp <= _to).ToList();
                        foreach (var tsItem in tsItems)
                        {
                            items.Add(new() { EntityIndex = tsItem.EntityId, Value = tsItem.Value, Timestamp = tsItem.ToDateTime });
                        }
                        tf.Close();
                    }
                }
            }
            catch (Exception ex) { OnExceptionOccurred(new(ex)); }
            return items;
        }



        // Method to raise the event
        protected virtual void OnExceptionOccurred(ExceptionEventArgs e)
        {
            ExceptionOccurred?.Invoke(this, e);
        }

        public void Dispose()
        {
            cancellationTokenSource.Cancel();
            writeTask?.Wait();
            dailyTimer?.Dispose();
        }

        private struct TeaItem
        {
            [EventTime]
            public Time Timestamp;
            public long EntityId;          //Entity Id
            public double Value;

            public DateTime ToDateTime { get { return (DateTime)Timestamp; } }
            public DateTime ToLocalDateTime { get { return ((DateTime)Timestamp).ToLocalTime(); } }
        }
    }
}
