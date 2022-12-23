using Delta.DeltaStructure.Common;
using Delta.DeltaStructure.Data;
using Delta.DeltaStructure.DeltaLog;

namespace Delta.DeltaStructure
{
    /// <summary>
    /// Delta table directory structure
    /// </summary>
    public class DeltaTable
    {
        internal string BasePath { get; private set; }
        internal DeltaLogDirectory? DeltaLog { get; private set; }
        internal DataFile[]? DataFiles { get; private set; }
        internal DataCrcFile[]? CrcFiles { get; private set; }
        internal Partition[] Partitions { get; private set; }
        internal List<IgnoredFile> IgnoredFileList { get; private set; }
        internal List<IgnoredDirectory> IgnoredDirectoryList { get; private set; }

        internal DeltaTable(string rootPath)
        {
            BasePath = rootPath;
            IgnoredFileList = new List<IgnoredFile>();
            IgnoredDirectoryList = new List<IgnoredDirectory>();
        }

        internal void SetDeltaLog(DeltaLogDirectory deltaLogDirectory) => DeltaLog = deltaLogDirectory;

        internal void SetRootData(DataFile[] dataFileList, DataCrcFile[] crcFileList)
        {
            DataFiles = dataFileList;
            CrcFiles = crcFileList;
        }

        internal void SetPartitions(Partition[] partitions) => Partitions = partitions;
    }
}