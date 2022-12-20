namespace Delta.Table
{
   internal class Partition
   {
      internal List<Partition> PartitionList = new();
      internal string? Key { get; }
      internal string? Value { get; }
      internal DataFile[]? DataFileList { get; }
      internal DataCrcFile[]? CrcFileList { get; }
      internal string Parent { get; }

      internal Partition(string parent, string? key = null, string? value = null, DataFile[]? dataFileList = null, DataCrcFile[]? crcFileList = null)
      {
         Key = key;
         Value = value;
         DataFileList = dataFileList;
         CrcFileList = crcFileList;
         Parent = parent;
      }
   }
}
