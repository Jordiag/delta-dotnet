using Delta.DeltaLog.Actions;

namespace Delta.DeltaLog
{
    // TODO needs to set sizes and mandatories removing nullables
    public class CheckPoint
    {
        internal Add[]? Adds { get; set; }
        internal Remove[]? Removes { get; set; }
        internal Txn[]? Txns { get; set; }
        internal CommitInfo[]? CommitInfos { get; set; }
        internal Version[]? Versions { get; set; }
        internal Protocol[]? Protocols { get; set; }
        internal Metadata[]? Metadatas { get; set; }
    }
}
