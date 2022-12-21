namespace Delta.Table
{
   public class DeltaTable
   {
      internal string BasePath { get; }
      internal DeltaLog? DeltaLog { get; private set; }
      internal DataFile[]? DataFiles { get; private set; }
      internal DataCrcFile[]? CrcFiles { get; private set; }
      internal Partition Partitions { get; private set; }
      internal List<IgnoredFile>? IgnoredFileList { get; private set; }
      internal List<IgnoredFolder> IgnoredFolderList { get; set; }

      internal DeltaTable(string rootPath)
      {
         BasePath = rootPath;
         IgnoredFolderList =  new List<IgnoredFolder>();
      }

      internal void LoadDeltaLog(DeltaLog deltaLogFolder)
      {
         DeltaLog = deltaLogFolder;
      }

      internal void LoadRootDataTable(DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile>? ignoredFileList)
      {
         DataFiles = dataFileList;
         CrcFiles = crcFileList;
         IgnoredFileList = ignoredFileList;
      }

      internal void AddIgnoredFolder(IgnoredFolder ignoredFolder)
      {
         IgnoredFolderList.Add(ignoredFolder);
      }
   }
}