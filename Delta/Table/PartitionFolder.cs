namespace Delta.Table
{
   internal class PartitionFolder
   {
      internal PartitionFolder? Folder { get; }
      internal string? Key { get; }
      internal string? Value { get; }
      internal DataFile[]? DataFileList { get; }
      internal DataCrcFile[]? CrcFileList { get; }

      internal PartitionFolder(string? key = null, string? value = null, DataFile[]? dataFileList = null, DataCrcFile[]? crcFileList = null, PartitionFolder? partitionFolder = null, PartitionDataFolder? dataFolder = null)
      {
         Key = key;
         Value = value;
         DataFileList = dataFileList;
         CrcFileList = crcFileList;
      }
   }
}
