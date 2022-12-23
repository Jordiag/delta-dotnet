using Delta.DeltaStructure.Base;
using Delta.DeltaStructure.Common;

namespace Delta.DeltaStructure.Data
{
    internal class DataFile : TableFile
    {
        public CompressionType CompressionType { get; }

        public DataFile(long index, string guid, CompressionType compressionType, long byteSize, string name) :
           base(index, guid, byteSize, name) => CompressionType = compressionType;
    }
}
