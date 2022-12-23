using Delta.DeltaStructure.Common;
using Delta.DeltaStructure.Data;
using Delta.DeltaStructure.DeltaLog;

namespace Delta.DeltaStructure
{
    /// <summary>
    /// Delta table folder structure
    /// </summary>
    public class DeltaTable
    {
        internal string BasePath { get; private set; }
        internal DeltaLogFolder? DeltaLog { get; private set; }
        internal DataFile[]? DataFiles { get; private set; }
        internal DataCrcFile[]? CrcFiles { get; private set; }
        internal Partition Partitions { get; private set; }
        internal List<IgnoredFile> IgnoredFileList { get; private set; }
        internal List<IgnoredFolder> IgnoredFolderList { get; private set; }

        internal DeltaTable(string rootPath)
        {
            BasePath = rootPath;
            IgnoredFileList = new List<IgnoredFile>();
            IgnoredFolderList = new List<IgnoredFolder>();
            Partitions = new Partition(rootPath);
        }

        internal void SetDeltaLog(DeltaLogFolder deltaLogFolder) => DeltaLog = deltaLogFolder;

        internal void SetRootData(DataFile[] dataFileList, DataCrcFile[] crcFileList)
        {
            DataFiles = dataFileList;
            CrcFiles = crcFileList;
        }
    }
}