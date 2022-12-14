namespace Delta.Table
{
   internal class DataFile : TableFile
   {
      public CompressionType CompressionType { get; }

      public DataFile(long partIndex, string guid, CompressionType compressionType, long byteSize) :
         base(partIndex, guid, byteSize) => CompressionType = compressionType;
   }
}
