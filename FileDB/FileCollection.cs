using IoTDBdotNET.Helper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    internal class FileCollection : BaseDatabase, IFileCollection
    {
        private readonly string _filesDirectory;
        public FileCollection(string dbPath, string containerName, string password = "", double backgroundTaskFromMilliseconds = 100) : base(dbPath, containerName, password, backgroundTaskFromMilliseconds)
        {
            _filesDirectory = Path.Combine(DbPath, "Files");
            Directory.CreateDirectory(_filesDirectory);
        }

        #region Add
        public Guid AddNewFile(string user, string filePath)
        {
            var fileId = Guid.NewGuid();
            CheckIn(user, fileId, null, filePath, true);

            return fileId;
        }

        public Guid AddFileFromStream(string user, Stream fileStream, string originalFileName)
        {
            var fileId = Guid.NewGuid();
            CheckIn(user, fileId, fileStream, null, true, originalFileName);

            return fileId;


            /* Example using AddFileFromStream
             * 
                [HttpPut("new")]
                public IActionResult NewFile(IFormFile uploadedFile)
                {
                    if (uploadedFile == null || uploadedFile.Length == 0)
                        return BadRequest("Add new file");

                    var fileId = IoTDb.Files.AddFileFromStream(stream, uploadedFile.FileName);
                    return Ok(new { FileId = fileId });
                
                }
             * 
             */
        }



        #endregion

        #region Access Logging
        private void LogFileAccess(string user, Guid fileId, FileOperation operation)
        {
            var accessLog = new FileAccessLog
            {
                FileId = fileId,
                UserName = user,
                AccessTime = DateTime.UtcNow,
                Operation = operation
            };

            var logsCollection = Database.GetCollection<FileAccessLog>("accessLogs");
            logsCollection.Insert(accessLog);
        }
        #endregion

        #region Check In/Out

        private void CheckIn (string user, Guid fileId, Stream? inputStream, string? filePath, bool isNew = false, string originalFileName = "")
        {
            if (inputStream == null && string.IsNullOrEmpty(filePath) && isNew) throw new InvalidOperationException("New file is null.");
            
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            Directory.CreateDirectory(fileDirectory);

  
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null)
            {
                if (isNew)
                {
                    fileMetadata = new();
                    fileMetadata.Id = fileId;
                }
                else
                {
                    throw new FileNotFoundException("File ID not found in database.");
                }
            }

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecord = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (checkoutRecord == null) //no checkout or new file
            {
                
                    checkoutRecord = new();
                
            }
            else if (!checkoutRecord.Username.Equals(user))
            {
                //checked out by another user
                throw new InvalidOperationException("No active checkout by this user.");
            }
            else
            { //checked out by current user

                if (isNew) //new but file already exist
                {
                    throw new InvalidOperationException("File exist: cannot add new file with same id.");
                }

            }
            var newVersion = fileMetadata.CurrentVersion + 1;

            if (string.IsNullOrEmpty(fileMetadata.FileName)) fileMetadata.FileName = Path.GetExtension(fileMetadata.FileName);
            if (string.IsNullOrEmpty(fileMetadata.FileExtension)) fileMetadata.FileExtension = Path.GetExtension(fileMetadata.FileName);
            var ext = fileMetadata.FileExtension ?? Path.GetExtension(fileMetadata.FileName);
            if (isNew)
            {
                if (string.IsNullOrEmpty(filePath))
                {
                    fileMetadata.FileName = originalFileName;
                    fileMetadata.FileExtension = Path.GetExtension(originalFileName);
                } else
                {
                    fileMetadata.FileName = Path.GetFileName(originalFileName);
                    fileMetadata.FileExtension = Path.GetExtension(filePath).Trim('.');
                }
            }
            if (string.IsNullOrEmpty(fileMetadata.FileName)) fileMetadata.FileName = "file";
            if (string.IsNullOrEmpty(fileMetadata.FileExtension)) fileMetadata.FileExtension = "dat";
            var fileName = $"{newVersion}.{fileMetadata.FileExtension}";
            
            var newFilePath = Path.Combine(fileDirectory, fileName);
            if (inputStream != null)
            {
                using (var fileStream = File.Create(newFilePath))
                {
                    inputStream.CopyTo(fileStream);
                }
            }
            else if (filePath != null)
            {
                File.Copy(filePath, newFilePath, true);
            }

            fileMetadata.CurrentVersion = newVersion;
            fileMetadata.Timestamp = DateTime.UtcNow;
            

            checkoutRecord.CheckinVersion = newVersion;
            checkoutRecord.Status = FileCheckoutStatus.Checkin;
            checkoutRecord.Timestamp = DateTime.UtcNow; // Update timestamp to reflect check-in time
            if (isNew)
            {
                filesCollection.Insert(fileMetadata.Id, fileMetadata);
                checkoutCollection.Insert(checkoutRecord);
                LogFileAccess(user, fileId, FileOperation.New);
            }
            else
            {
                filesCollection.Update(fileMetadata);
                checkoutCollection.Update(checkoutRecord);
                LogFileAccess(user, fileId, FileOperation.CheckIn);
            }
            
            
        }
        public void CheckInFile(string user, Guid fileId, string filePath)
        {
            CheckIn(user, fileId, null, filePath);
        }


        public void CheckInFileFromStream(string user, Guid fileId, Stream inputStream)
        {
            CheckIn(user, fileId, inputStream, null);

            /* Example using CheckInFileFromStream
             * 
                [HttpPost("checkin/{fileId}")]
                public async Task<IActionResult> CheckInFile(Guid fileId, [FromForm] IFormFile file)
                {
                    if (file == null || file.Length == 0)
                    {
                        return BadRequest("No file uploaded.");
                    }

                    var username = User.Identity.Name; // Assuming authentication is in place

                    using (var stream = file.OpenReadStream())
                    {
                        IoTDb.Files.CheckInFileFromStream(username, fileId, stream);
                    }

                    return Ok("File uploaded successfully.");
                }
             * 
             */
        }

        public FileMetadata CheckOutFile(string user, Guid fileId, out string filePath, int? version = null)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            // Ensure only one checkout at a time
            var existingCheckout = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (existingCheckout != null)
            {
                //throw error if checked out by another use. 
                if (!existingCheckout.Username.Equals(user, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("File is already checked out.");
                }
                //at this point, it  is the same checked out user. Check if checked out version is the same.
                else if ((version != null && version > 0) && existingCheckout.CheckoutVersion != version)
                {
                    throw new InvalidOperationException($"User [{user}] checked out file version [{existingCheckout.CheckoutVersion}] on [{existingCheckout.Timestamp}].");
                }

            }

            //ok to send file to user
            var fileVersion = version ?? fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var ext = fileMetadata.FileExtension ?? Path.GetExtension(fileMetadata.FileName);
            filePath = Path.Combine(fileDirectory, $"{fileVersion}.{ext}");

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Requested file version does not exist.");
            }

            if (existingCheckout == null)
            {
                var newCheckoutRecord = new FileCheckoutRecord
                {
                    FileId = fileId,
                    CheckoutVersion = fileVersion,
                    Username = user,
                    Timestamp = DateTime.UtcNow,
                    Status = FileCheckoutStatus.Checkout
                };
                checkoutCollection.Insert(newCheckoutRecord);
            }
            LogFileAccess(user, fileId, FileOperation.CheckOut);

            return fileMetadata;
        }
        public FileMetadata CheckOutFileToStream(string user, Guid fileId, Stream outputStream, int? version = null)
        {

            var fileMetadata = CheckOutFile(user, fileId, out string filePath, version);

            using (var fileStream = File.OpenRead(filePath))
            {
                fileStream.CopyTo(outputStream);
            }

            return fileMetadata;
            /* Example using CheckOutFileToStream
             * 
                [HttpGet("checkout/{fileId}")]
                public IActionResult CheckOutFile(Guid fileId)
                {
                    var username = User.Identity.Name; // Assuming authentication is in place

                    // Create a MemoryStream as a placeholder for the file's data
                    var outputStream = new MemoryStream();
                    IoTDb.Files.CheckOutFileToStream(username, fileId, outputStream);

                    // Reset the position of the MemoryStream to the beginning
                    outputStream.Position = 0;

                    // Assuming the file's name is stored and accessible, here simply using fileId for demonstration
                    string fileName = $"{fileId}.dat";

                    // Return the file stream to the client
                    return File(outputStream, "application/octet-stream", fileName);
                }

             * 
             */
        }

        public void AbandonCheckout(string user, Guid fileId, bool force = false)
        {
            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecord = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (checkoutRecord == null)
            {
                return;
            }
            else if (checkoutRecord.Username != user && !force)
            {
                throw new InvalidOperationException("Cannot abandon checkout by other user.");
            }

            checkoutRecord.Status = FileCheckoutStatus.Abandon;
            checkoutRecord.Timestamp = DateTime.UtcNow; // Update timestamp to reflect abandon time
            checkoutCollection.Update(checkoutRecord);

            LogFileAccess(user, fileId, FileOperation.AbandonCheckout);
        }

        #endregion

        #region C
        /// <summary>
        /// Get document count using property on collection.
        /// </summary>
        long Count()
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount();
        }

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long Count(BsonExpression predicate)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount(predicate);
        }

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long Count(string predicate, BsonDocument parameters)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount(predicate, parameters);
        }

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long Count(string predicate, params BsonValue[] args)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount(predicate, args);
        }

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long Count(Expression<Func<FileMetadata, bool>> predicate)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount(predicate);
        }

        /// <summary>
        /// Count documents matching a query. This method does not deserialize any documents. Needs indexes on query expression
        /// </summary>
        long Count(Query query)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.LongCount(query);
        }


        #endregion

        #region D

        public void DeleteFile(string user, Guid fileId)
        {

            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) return; // File doesn't exist, no action needed

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            // Is file checked out?
            var existingCheckout = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (existingCheckout != null)
            {
                //throw error if checked out by another use. 
                if (!existingCheckout.Username.Equals(user, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("File is already checked out.");
                }
            }

            // Optional: Delete physical files
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            if (Directory.Exists(fileDirectory))
            {
                Directory.Delete(fileDirectory, true); // true for recursive deletion
            }

            // Delete metadata from database
            filesCollection.Delete(fileId);

            LogFileAccess(user, fileId, FileOperation.Delete);
        }


        #endregion

        #region F

        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        List<FileMetadata> Find(BsonExpression predicate, int skip = 0, int limit = int.MaxValue)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.Find(predicate, skip, limit).ToList();
        }

        /// <summary>
        /// Find documents inside a collection using query definition.
        /// </summary>
        List<FileMetadata> Find(Query query, int skip = 0, int limit = int.MaxValue)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.Find(query, skip, limit).ToList();
        }

        /// <summary>
        /// Find documents inside a collection using predicate expression.
        /// </summary>
        List<FileMetadata> Find(Expression<Func<FileMetadata, bool>> predicate, int skip = 0, int limit = int.MaxValue)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.Find(predicate, skip, limit).ToList();
        }

        /// <summary>
        /// Find a document using Document Id. Returns null if not found.
        /// </summary>
        FileMetadata FindById(BsonValue fileId)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindById(fileId);
        }

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        FileMetadata FindOne(BsonExpression predicate)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindOne(predicate);
        }

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        FileMetadata FindOne(string predicate, BsonDocument parameters)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindOne(predicate, parameters);
        }

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        FileMetadata FindOne(BsonExpression predicate, params BsonValue[] args)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindOne(predicate, args);
        }

        /// <summary>
        /// Find the first document using predicate expression. Returns null if not found
        /// </summary>
        FileMetadata FindOne(Expression<Func<FileMetadata, bool>> predicate)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindOne(predicate);
        }

        /// <summary>
        /// Find the first document using defined query structure. Returns null if not found
        /// </summary>
        FileMetadata FindOne(Query query)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindOne(query);
        }

        /// <summary>
        /// Returns all documents inside collection order by _id index.
        /// </summary>
        List<FileMetadata> FindAll()
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            return filesCollection.FindAll().ToList();
        }

        #endregion

        #region G
        public FileMetadata GetFilePath(string user, Guid fileId, out string filePath)
        {

            var fileMetadata = GetFileMetadata(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var fileVersion = fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var ext = fileMetadata.FileExtension ?? Path.GetExtension(fileMetadata.FileName);
            filePath = Path.Combine(fileDirectory, $"{fileVersion}.{ext}");

            LogFileAccess(user, fileId, FileOperation.Get); // Assuming FileOperation enum includes a Get operation

            return fileMetadata;
        }

        public FileMetadata? GetFileMetadata(Guid fileId)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);

            return fileMetadata;
        }


        public FileMetadata GetFileToStream(string user, Guid fileId, Stream outputStream)
        {
            var fileMetadata = GetFilePath(user, fileId, out string filePath);

            using (var fileStream = File.OpenRead(filePath))
            {
                fileStream.CopyTo(outputStream);
            }

            LogFileAccess(user, fileId, FileOperation.Get); // Log the get operation
            return fileMetadata;
        }

        public List<FileMetadata> GetFiles()
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadatas = filesCollection.FindAll().ToList();
            return fileMetadatas;
        }

        public FileRecord GetFileRecord(Guid fileId)
        {
            // Fetch file metadata
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File metadata not found.");

            // Fetch access logs related to the file
            var accessLogsCollection = Database.GetCollection<FileAccessLog>("accessLogs");
            var accessLogs = accessLogsCollection.Find(log => log.FileId == fileId).ToList();

            // Fetch checkout records related to the file
            var checkoutRecordsCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecords = checkoutRecordsCollection.Find(record => record.FileId == fileId).ToList();

            // Compile file versions into a hierarchical structure
            var fileVersions = CompileFileVersions(checkoutRecords);

            // Assemble the FileRecord
            var fileRecord = new FileRecord
            {
                Metadata = fileMetadata,
                AccessLog = accessLogs,
                CheckoutRecords = checkoutRecords,
                FileVersions = fileVersions
            };

            return fileRecord;
        }

        private List<FileVersionNode> CompileFileVersions(List<FileCheckoutRecord> checkoutRecords)
        {
            // Initialize an empty list for root nodes
            List<FileVersionNode> rootNodes = new List<FileVersionNode>();

            // Dictionary to keep track of all nodes by version to easily find and set children
            Dictionary<int, FileVersionNode> allNodes = new Dictionary<int, FileVersionNode>();

            // Populate the dictionary and identify root nodes
            foreach (var record in checkoutRecords)
            {
                var node = new FileVersionNode
                {
                    Version = record.CheckinVersion, // Assuming this represents the resulting version after check-in
                    Children = new List<FileVersionNode>()
                };

                if (record.CheckoutVersion == 0 || !allNodes.ContainsKey(record.CheckoutVersion))
                {
                    // This is a root node
                    rootNodes.Add(node);
                }

                allNodes[node.Version] = node;
            }

            // Set children for each node
            foreach (var record in checkoutRecords)
            {
                if (allNodes.ContainsKey(record.CheckoutVersion))
                {
                    var parentNode = allNodes[record.CheckoutVersion];
                    parentNode.Children.Add(allNodes[record.CheckinVersion]);
                }
            }

            return rootNodes;
        }


        #endregion

        #region R
        public void RenameFile(string user, Guid fileId, string newUserFileName)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            // Extract the new file name and extension
            var newFileName = Path.GetFileNameWithoutExtension(newUserFileName);
            var newFileExtension = Path.GetExtension(newUserFileName);

            // Update metadata
            fileMetadata.FileName = newFileName;
            fileMetadata.FileExtension = newFileExtension; // Assuming the property is named FileExtension
            filesCollection.Update(fileMetadata);

            LogFileAccess(user, fileId, FileOperation.Rename);
        }

        #endregion

        #region Base Overrride

        protected override void InitializeDatabase()
        {
            throw new NotImplementedException();
        }

        protected override void PerformBackgroundWork(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
