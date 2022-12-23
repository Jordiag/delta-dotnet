using Delta.DeltaStructure;

namespace Delta.DeltaStructure.Base
{
    abstract class TableFile : IndexedFile
    {
        internal string Guid { get; }
        internal long ByteSize { get; }

        protected TableFile(long Index, string guid, long byteSize, string name)
            : base(Index, name)
        {
            Guid = guid;
            ByteSize = byteSize;
        }
    }
}
