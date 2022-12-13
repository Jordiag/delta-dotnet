namespace Delta.Table
{
   public class DataFile
   {
      public long PartIndex { get; }
      public string Guid { get; }
      public FileType FileType { get; }
      public bool IsCheckpoint { get; }
      public CompressionType CompressionType { get; }

      public DataFile(long partIndex, string guid, FileType fileType, bool isCheckpoint, CompressionType compressionType)
      {
         PartIndex = partIndex;
         Guid = guid;
         FileType = fileType;
         IsCheckpoint = isCheckpoint;
         CompressionType = compressionType;
      }
   }
}
