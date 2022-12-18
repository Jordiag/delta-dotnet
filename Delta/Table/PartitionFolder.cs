namespace Delta.Table
{
   internal class PartitionFolder
   {
      internal List<PartitionFolder> FolderList = new();
      internal string? Key { get; }
      internal string? Value { get; }
      internal DataFile[]? DataFileList { get; }
      internal DataCrcFile[]? CrcFileList { get; }
      internal string Parent { get; }

      internal PartitionFolder(string parent, string? key = null, string? value = null, DataFile[]? dataFileList = null, DataCrcFile[]? crcFileList = null)
      {
         Key = key;
         Value = value;
         DataFileList = dataFileList;
         CrcFileList = crcFileList;
         Parent = parent;
      }
   }
}
