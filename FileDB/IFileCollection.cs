namespace IoTDBdotNET.FileDB
{
    public interface IFileCollection
    {
        event EventHandler<ExceptionEventArgs>? ExceptionOccurred;
        void AbandonCheckout(string user, Guid fileId, bool force = false);
        Guid AddFileFromStream(string user, Stream fileStream, string originalFileName);
        Guid AddNewFile(string user, string filePath);
        void CheckInFile(string user, Guid fileId, string filePath);
        void CheckInFileFromStream(string user, Guid fileId, Stream inputStream);
        FileMetadata CheckOutFile(string user, Guid fileId, out string filePath, int? version = null);
        FileMetadata CheckOutFileToStream(string user, Guid fileId, Stream outputStream, int? version = null);
        void DeleteFile(string user, Guid fileId);
        FileMetadata GetFilePath(string user, Guid fileId, out string filePath);
        FileRecord GetFileRecord(Guid fileId);
        List<FileMetadata> GetFiles();
        FileMetadata? GetFileMetadata(Guid fileId);
        FileMetadata GetFileToStream(string user, Guid fileId, Stream outputStream);
        void RenameFile(string user, Guid fileId, string newUserFileName);
    }
}