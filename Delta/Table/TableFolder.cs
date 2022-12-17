namespace Delta.Table
{
   internal class TableFolder
   {
      internal string RootPath { get; }
      internal LogFolder DeltaLogFolder { get; }
      internal DataFile[] DataFileList { get; }
      internal DataCrcFile[] CrcFileList { get; }
      internal List<PartitionFolder> PartitionFolderList { get; }

      internal TableFolder(string rootPath, LogFolder deltaLogFolder, DataFile[] dataFileList, DataCrcFile[] crcFileList, List<PartitionFolder> partitionFolderList)
      {
         RootPath = rootPath;
         DeltaLogFolder = deltaLogFolder;
         DataFileList = dataFileList;
         CrcFileList = crcFileList;
         PartitionFolderList = partitionFolderList;
      }
   }
}
