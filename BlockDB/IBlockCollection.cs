namespace IoTDBdotNET.BlockDB
{
    public interface IBlockCollection
    {
        event EventHandler<ExceptionEventArgs>? ExceptionOccurred;
        long Count();
        Block? Get();
        List<Block>? Get(DateTime startDate, DateTime endDate);
        List<Block>? Get(int count);
        bool IsBlockConsistent(DateTime startDate, DateTime endDate);
        bool IsBlockConsistent(int count);
        List<(Block Block, bool Valid)> VerifyBlockConsistency(DateTime startDate, DateTime endDate);
        List<(Block Block, bool Valid)> VerifyBlockConsistency(int count);
    }
}