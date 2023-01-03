using Delta.DeltaLog.Actions;

namespace Delta.DeltaLog
{
    // TODO needs to set sizes and mandatories removing nullables
    public class CheckPoint
    {
        public List<Cdc> Cdcs { get; set; }
        public List<Add> Adds { get; set; }
        public List<Remove> Removes { get; set; }
        public List<Txn> Txns { get; set; }
        public List<CommitInfo> CommitInfos { get; set; }
        public List<Version> Versions { get; set; }
        public List<Protocol> Protocols { get; set; }
        public List<Metadata> Metadatas { get; set; }

        public CheckPoint()
        {
            Cdcs = new List<Cdc>();
            Adds = new List<Add>();
            Removes = new List<Remove>();
            Txns = new List<Txn>();
            CommitInfos = new List<CommitInfo>();
            Versions = new List<Version>();
            Protocols = new List<Protocol>();
            Metadatas = new List<Metadata>();
        }
    }
}
