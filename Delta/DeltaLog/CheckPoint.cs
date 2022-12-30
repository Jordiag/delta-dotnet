using Delta.DeltaLog.Actions;

namespace Delta.DeltaLog
{
    // TODO needs to set sizes and mandatories removing nullables
    public class CheckPoint
    {
        public Cdc[] Cdcs { get; internal set; }
        internal IEnumerable<Add> Adds { get; set; }
        internal IEnumerable<Remove>? Removes { get; set; }
        internal IEnumerable<Txn>? Txns { get; set; }
        internal IEnumerable<CommitInfo>? CommitInfos { get; set; }
        internal IEnumerable<Version>? Versions { get; set; }
        internal IEnumerable<Protocol>? Protocols { get; set; }
        internal IEnumerable<Metadata>? Metadatas { get; set; }
    }
}
