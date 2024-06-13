using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    public class TemporaryFileStorage
    {
        private readonly string _tempDirectory;
        private readonly ConcurrentDictionary<string, Timer> _expirationTimers;

        public TemporaryFileStorage(string tempDirectory)
        {
            _tempDirectory = tempDirectory;
            _expirationTimers = new ConcurrentDictionary<string, Timer>();
            Directory.CreateDirectory(_tempDirectory);
        }

        public string CreateTemporaryFile(Stream content, TimeSpan expiration)
        {
            string fileName = Guid.NewGuid().ToString();
            string filePath = Path.Combine(_tempDirectory, fileName);

            using (var fileStream = File.Create(filePath))
            {
                content.CopyTo(fileStream);
            }

            var timer = new Timer(DeleteFile, filePath, expiration, Timeout.InfiniteTimeSpan);
            _expirationTimers[fileName] = timer;

            return filePath;
        }

        private void DeleteFile(object state)
        {
            string filePath = (string)state;
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
                _expirationTimers.TryRemove(Path.GetFileName(filePath), out _);
            }
        }
    }
}
