using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.TimeSeriesDB
{
    internal class FileLocker<T> where T : struct, IDisposable
    {
        public enum Mode
        {
            Read,
            Write,
            ReadWrite
        }

        private readonly object _fileLockObject = new object();

        private Mode FileMode;
        private Mode OperatingMode;
        private TeaFile<T>? Tf = null;
        private string FilePath;
        private readonly string _password;

        public FileLocker(Mode mode, string path, string? password) 
        {
            FileMode = mode;
            FilePath = path;
            _password = "";
            if (!string.IsNullOrEmpty(password) ) _password = password;
        }

        public IItemCollection<T>? Read
        {
            get
            {
                if (!SetMode(Mode.Read)) return null;
                return Tf?.Items;
            }
        }
        public bool Write(T item)
        {
            try
            {
                if (!SetMode(Mode.Write)) return false;
                Tf?.Write(item);
                return true;
            }
            catch { }
            return false;
        }

        public bool Write(IEnumerable<T> items)
        {
            try
            {
                if (FileMode == Mode.Read)
                {
                    return false;
                }
                Tf?.Write(items);
                return true;
            }
            catch { }
            return false;
        }

        
        private bool SetMode(Mode mode)
        {
            if (mode == Mode.ReadWrite) return false;
            if (mode == Mode.Write)
            {
                if (FileMode == Mode.Read)
                {
                    return false;
                }
                if (OperatingMode == Mode.Read)
                {
                    Tf?.Dispose();
                    Tf = null;
                }
                if (Tf == null)
                {
                    if (File.Exists(FilePath))
                    {
                        Tf = TeaFile<T>.Append(FilePath, _password);
                    }
                    else
                    {
                        Tf = TeaFile<T>.Create(FilePath, _password);
                    }
                }
                OperatingMode = Mode.Write;
            } else
            {
                if (FileMode == Mode.Write || !File.Exists(FilePath))
                {
                    return false;
                }
                if (Tf == null)
                {
                    Tf = TeaFile<T>.OpenRead(FilePath, _password);
                }
                else if (OperatingMode == Mode.Write)
                {
                    Tf.Dispose();
                    Tf = TeaFile<T>.OpenRead(FilePath, _password);
                }
                OperatingMode = Mode.Read;
            }
            return true;
        }

        public void Dispose ()
        {
            Tf?.Dispose();
        }
    }
}
