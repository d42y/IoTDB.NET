using IoTDBdotNET.Helper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET.FileDB
{
    internal class FileCollection : BaseDatabase, IFileCollection
    {
        private readonly string _filesDirectory;
        public FileCollection(string dbPath, string containerName, double backgroundTaskFromMilliseconds = 100) : base(dbPath, containerName, backgroundTaskFromMilliseconds)
        {
            _filesDirectory = Path.Combine(DbPath, "Files");
            Directory.CreateDirectory(_filesDirectory);
        }

        #region Add
        public Guid AddNewFile(string user, string filePath)
        {
            var fileId = Guid.NewGuid();
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            Directory.CreateDirectory(fileDirectory);

            var version = 1;
            var fileName = Path.GetFileNameWithoutExtension(filePath);
            var extension = Path.GetExtension(filePath);
            var newFilePath = Path.Combine(fileDirectory, $"{version}.{extension}");
            File.Copy(filePath, newFilePath);


            var filesCollection = Database.GetCollection<FileMetadata>("files");
            filesCollection.Insert(new FileMetadata
            {
                Id = fileId,
                CurrentVersion = version,
                FileName = fileName,
                FileExtension = extension
            });

            var newCheckinRecord = new FileCheckoutRecord
            {
                FileId = fileId,
                CheckoutVersion = 0,
                CheckinVersion = version,
                Username = user,
                Timestamp = DateTime.UtcNow,
                Status = FileCheckoutStatus.Checkin
            };
            Database.GetCollection<FileCheckoutRecord>("checkoutRecords").Insert(newCheckinRecord);

            LogFileAccess(user, fileId, FileOperation.New);

            return fileId;
        }

        public Guid AddFileFromStream(string user, Stream fileStream, string originalFileName)
        {
            var fileId = Guid.NewGuid();
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            Directory.CreateDirectory(fileDirectory);

            var version = 1;
            var fileName = Path.GetFileNameWithoutExtension(originalFileName);
            var extension = Path.GetExtension(originalFileName);
            var newFilePath = Path.Combine(fileDirectory, $"{version}.{extension}");
            using (var fileOutput = File.Create(newFilePath))
            {
                fileStream.CopyTo(fileOutput);
            }


            var filesCollection = Database.GetCollection<FileMetadata>("files");
            filesCollection.Insert(new FileMetadata { Id = fileId, CurrentVersion = version, FileName = originalFileName });

            var newCheckinRecord = new FileCheckoutRecord
            {
                FileId = fileId,
                CheckoutVersion = 0,
                CheckinVersion = version,
                Username = user,
                Timestamp = DateTime.UtcNow,
                Status = FileCheckoutStatus.Checkin
            };
            Database.GetCollection<FileCheckoutRecord>("checkoutRecords").Insert(newCheckinRecord);

            LogFileAccess(user, fileId, FileOperation.New);

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
        public void CheckInFile(string user, Guid fileId, string filePath)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");


            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecord = checkoutCollection.FindOne(x => x.FileId == fileId && x.Username == user && x.Status == FileCheckoutStatus.Checkout);
            if (checkoutRecord == null)
            {
                throw new InvalidOperationException("No active checkout by this user.");
            }

            var newVersion = fileMetadata.CurrentVersion + 1;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var fileName = $"{newVersion}.{fileMetadata.FileExtension}";
            var newFilePath = Path.Combine(fileDirectory, fileName);
            File.Copy(filePath, newFilePath, true);

            fileMetadata.CurrentVersion = newVersion;
            fileMetadata.Timestamp = DateTime.UtcNow;
            filesCollection.Update(fileMetadata);

            checkoutRecord.CheckinVersion = newVersion;
            checkoutRecord.Status = FileCheckoutStatus.Checkin;
            checkoutRecord.Timestamp = DateTime.UtcNow; // Update timestamp to reflect check-in time
            checkoutCollection.Update(checkoutRecord);

            LogFileAccess(user, fileId, FileOperation.CheckIn);
        }

        public string CheckOutFile(string user, Guid fileId, int? version = null)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            // Ensure only one checkout at a time
            var existingCheckout = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (existingCheckout != null)
            {
                throw new InvalidOperationException("File is already checked out.");
            }


            var fileVersion = version ?? fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var filePath = Path.Combine(fileDirectory, $"{fileVersion}.{fileMetadata.FileExtension}");

            var newCheckoutRecord = new FileCheckoutRecord
            {
                FileId = fileId,
                CheckoutVersion = fileVersion,
                Username = user,
                Timestamp = DateTime.UtcNow,
                Status = FileCheckoutStatus.Checkout
            };
            checkoutCollection.Insert(newCheckoutRecord);


            LogFileAccess(user, fileId, FileOperation.CheckOut);

            return filePath;
        }

        public void CheckInFileFromStream(string user, Guid fileId, Stream inputStream)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecord = checkoutCollection.FindOne(x => x.FileId == fileId && x.Username == user && x.Status == FileCheckoutStatus.Checkout);
            if (checkoutRecord == null)
            {
                throw new InvalidOperationException("No active checkout by this user.");
            }

            var newVersion = fileMetadata.CurrentVersion + 1;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var fileName = $"{newVersion}.{fileMetadata.FileExtension}";
            var newFilePath = Path.Combine(fileDirectory, fileName);

            using (var fileStream = File.Create(newFilePath))
            {
                inputStream.CopyTo(fileStream);
            }

            fileMetadata.CurrentVersion = newVersion;
            fileMetadata.Timestamp = DateTime.UtcNow;
            filesCollection.Update(fileMetadata);


            checkoutRecord.CheckinVersion = newVersion;
            checkoutRecord.Status = FileCheckoutStatus.Checkin;
            checkoutRecord.Timestamp = DateTime.UtcNow; // Update timestamp to reflect check-in time
            checkoutCollection.Update(checkoutRecord);

            LogFileAccess(user, fileId, FileOperation.CheckIn);

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

        public FileMetadata CheckOutFileToStream(string user, Guid fileId, Stream outputStream, int? version = null)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            // Ensure only one checkout at a time
            var existingCheckout = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (existingCheckout != null)
            {
                throw new InvalidOperationException("File is already checked out.");
            }

            var fileVersion = version ?? fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var filePath = Path.Combine(fileDirectory, $"{fileVersion}.{fileMetadata.FileExtension}");

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Requested file version does not exist.");
            }

            using (var fileStream = File.OpenRead(filePath))
            {
                fileStream.CopyTo(outputStream);
            }

            var newCheckoutRecord = new FileCheckoutRecord
            {
                FileId = fileId,
                CheckoutVersion = fileVersion,
                Username = user,
                Timestamp = DateTime.UtcNow,
                Status = FileCheckoutStatus.Checkout
            };
            checkoutCollection.Insert(newCheckoutRecord);

            LogFileAccess(user, fileId, FileOperation.CheckOut);

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

        public void AbandonCheckout(string user, Guid fileId)
        {
            var checkoutCollection = Database.GetCollection<FileCheckoutRecord>("checkoutRecords");
            var checkoutRecord = checkoutCollection.FindOne(x => x.FileId == fileId && x.Status == FileCheckoutStatus.Checkout);
            if (checkoutRecord == null || checkoutRecord.Username != user)
            {
                throw new InvalidOperationException("No active checkout by this user to abandon.");
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
        public string GetFilePath(string user, Guid fileId)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var fileVersion = fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var filePath = Path.Combine(fileDirectory, $"{fileVersion}.{fileMetadata.FileExtension}");

            LogFileAccess(user, fileId, FileOperation.Get); // Assuming FileOperation enum includes a Get operation

            return filePath;
        }

        public void GetFileToStream(string user, Guid fileId, Stream outputStream)
        {
            var filesCollection = Database.GetCollection<FileMetadata>("files");
            var fileMetadata = filesCollection.FindById(fileId);
            if (fileMetadata == null) throw new FileNotFoundException("File ID not found in database.");

            var fileVersion = fileMetadata.CurrentVersion;
            var fileDirectory = Path.Combine(_filesDirectory, fileId.ToString());
            var filePath = Path.Combine(fileDirectory, $"{fileVersion}.{fileMetadata.FileExtension}");

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Requested file version does not exist.");
            }

            using (var fileStream = File.OpenRead(filePath))
            {
                fileStream.CopyTo(outputStream);
            }

            LogFileAccess(user, fileId, FileOperation.Get); // Log the get operation
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
