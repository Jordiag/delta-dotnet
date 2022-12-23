using Delta.DeltaStructure.Base;

namespace Delta.DeltaStructure.DeltaLog
{
    internal class LogFile : IndexedFile
    {
        internal LogFile(long index, string name)
            : base(index, name)
        {
        }
    }
}