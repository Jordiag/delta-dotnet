namespace Delta.Table
{
   internal class DataCrcFile : TableFile
   {
      public DataCrcFile(long partIndex, string guid, long byteSize, string name) :
         base(partIndex, guid, byteSize, name)
      { }
   }
}
