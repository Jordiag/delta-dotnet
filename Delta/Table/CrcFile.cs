namespace Delta.Table
{
   internal class CrcFile : TableFile
   {
      public CrcFile(long partIndex, string guid, long byteSize) :
         base(partIndex, guid, byteSize)
      {}
   }
}
