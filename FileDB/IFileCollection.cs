namespace IoTDBdotNET.FileDB
{
    public interface IFileCollection
    {
        event EventHandler<ExceptionEventArgs>? ExceptionOccurred;
        void AbandonCheckout(string user, Guid fileId);
        Guid AddFileFromStream(string user, Stream fileStream, string originalFileName);
        Guid AddNewFile(string user, string filePath);
        void CheckInFile(string user, Guid fileId, string filePath);
        void CheckInFileFromStream(string user, Guid fileId, Stream inputStream);
        string CheckOutFile(string user, Guid fileId, int? version = null);
        FileMetadata CheckOutFileToStream(string user, Guid fileId, Stream outputStream, int? version = null);
        void DeleteFile(string user, Guid fileId);
        string GetFilePath(string user, Guid fileId);
        FileRecord GetFileRecord(Guid fileId);
        List<FileMetadata> GetFiles();
        void GetFileToStream(string user, Guid fileId, Stream outputStream);
        void RenameFile(string user, Guid fileId, string newUserFileName);
    }
}