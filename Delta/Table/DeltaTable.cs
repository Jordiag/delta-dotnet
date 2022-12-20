namespace Delta.Table
{
   internal class DeltaTable
   {
      internal string BasePath { get; }
      internal LogFolder? DeltaLogFolder { get; private set; }
      internal DataFile[]? DataFileList { get; private set; }
      internal DataCrcFile[]? CrcFileList { get; private set; }
      internal Partition Partitions { get; private set; }
      internal List<IgnoredFile>? IgnoredFileList { get; private set; }
      internal List<IgnoredFolder> IgnoredFolderList { get; set; }

      internal DeltaTable(string rootPath)
      {
         BasePath = rootPath;
         IgnoredFolderList =  new List<IgnoredFolder>();
      }

      internal void LoadDeltaLog(LogFolder deltaLogFolder)
      {
         DeltaLogFolder = deltaLogFolder;
      }

      internal void LoadRootDataTable(DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile>? ignoredFileList)
      {
         DataFileList = dataFileList;
         CrcFileList = crcFileList;
         IgnoredFileList = ignoredFileList;
      }

      internal void AddIgnoredFolder(IgnoredFolder ignoredFolder)
      {
         IgnoredFolderList.Add(ignoredFolder);
      }
   }
}