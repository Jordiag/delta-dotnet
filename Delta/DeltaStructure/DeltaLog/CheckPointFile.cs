using Delta.DeltaStructure.Base;

namespace Delta.DeltaStructure.DeltaLog
{
    internal class CheckPointFile : IndexedFile
    {
        internal CheckPointFile(long index, string name)
            : base(index, name)
        {
        }
    }
}