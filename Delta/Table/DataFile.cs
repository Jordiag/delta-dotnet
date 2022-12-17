namespace Delta.Table
{
   internal class DataFile : TableFile
   {
      public CompressionType CompressionType { get; }

      public DataFile(long partIndex, string guid, CompressionType compressionType, long byteSize, string name) :
         base(partIndex, guid, byteSize, name) => CompressionType = compressionType;
   }
}
