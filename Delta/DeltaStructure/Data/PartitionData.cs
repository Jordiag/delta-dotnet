namespace Delta.DeltaStructure.Data
{
    internal class PartitionData : Partition
    {
        internal DataFile[]? DataFiles { get; }
        internal DataCrcFile[]? CrcFiles { get; }

        internal PartitionData(string parent, string? key = null, string? value = null, DataFile[]? dataFileList = null, DataCrcFile[]? crcFileList = null)
            : base(parent, key, value)
        {
            DataFiles = dataFileList;
            CrcFiles = crcFileList;
        }
    }
}
