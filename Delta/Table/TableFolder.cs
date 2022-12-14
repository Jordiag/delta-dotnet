namespace Delta.Table
{
   public class TableFolder
   {
      public string RootPath { get; }

      public TableLogFolder DeltaLogFolder { get; set; }
      internal DataFile[] DataFileList { get; set; }
      internal CrcFile[] CrcFileList { get; set; }
      public PartitionFolder[] PartitionFolderList { get; set; }

      public TableFolder(string fullPath) => RootPath = fullPath;
   }
}
