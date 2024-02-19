using System.Security.Cryptography;
using System.Text;

namespace IoTDBdotNET
{
    internal abstract class BaseDatabase : IDisposable
    {
        public event EventHandler<ExceptionEventArgs> ExceptionOccurred;
        private readonly LiteDatabase _db;
        private readonly int _numThreads;
        protected int NumThreads { get { return _numThreads; } }
        private readonly object _syncRoot = new object();
        private CancellationTokenSource _cancellationTokenSource = new();
        private Task? _backgroundTask;
        private readonly double _backgroundTaskFromMilliseconds;
        public BaseDatabase(string dbPath, string dbName, double backgroundTaskFromMilliseconds = 100)
        {
            int logicalProcessorCount = Environment.ProcessorCount;
            _numThreads = logicalProcessorCount > 1 ? logicalProcessorCount - 1 : 1;
            _db = new LiteDatabase(Path.Combine(dbPath, $"{dbName}.db"));
            _backgroundTaskFromMilliseconds = backgroundTaskFromMilliseconds;
            StartBackgroundTask();
        }

        protected LiteDatabase Database { get { return _db; } }
        protected CancellationTokenSource CancellationTokenSource { get { return _cancellationTokenSource; } }
        protected object SyncRoot { get { return _syncRoot; } }
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
            _db?.Dispose();
        }
    }
}
