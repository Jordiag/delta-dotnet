namespace Delta.Table
{
   public class TableFolder
   {
      public string RootPath { get; }

      public TableLogFolder DeltaLogFolder { get; set; }
      public DataFile[] DataFileList { get; set; }
      public PartitionFolder[] PartitionFolderList { get; set; }

      public TableFolder(string fullPath) => RootPath = fullPath;
   }
}
