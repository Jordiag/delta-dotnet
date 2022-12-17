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
      public string Name { get; }

      protected TableFile(long partIndex, string guid, long byteSize, string name)
      {
         PartIndex = partIndex;
         Guid = guid;
         ByteSize = byteSize;
         Name = name;
      }
   }
}
