namespace Delta.Table
{
   /// <summary>
   /// Parquet File Containnig table data
   /// </summary>
   abstract class TableFile
   {
      public long PartIndex { get; }
      public string Guid { get; }
      public long ByteSize { get; }

      protected TableFile(long partIndex, string guid, long byteSize)
      {
         PartIndex = partIndex;
         Guid = guid;
         ByteSize = byteSize;
      }
   }
}
