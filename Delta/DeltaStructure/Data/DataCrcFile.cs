using Delta.DeltaStructure.Base;

namespace Delta.DeltaStructure.Data
{
    internal class DataCrcFile : TableFile
    {
        public DataCrcFile(long index, string guid, long byteSize, string name) :
           base(index, guid, byteSize, name)
        { }
    }
}
