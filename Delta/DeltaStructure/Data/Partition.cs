namespace Delta.DeltaStructure.Data
{
    internal class Partition
    {
        internal List<Partition> PartitionList { get; }
        internal string? Key { get; }
        internal string? Value { get; }
        internal string Parent { get; }

        internal Partition(string parent, string? key = null, string? value = null)
        {
            Key = key;
            Value = value;
            Parent = parent;
            PartitionList = new();
        }
    }
}
