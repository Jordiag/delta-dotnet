namespace Delta.DeltaLog
{
    public class Add
    {
        public string path { get; set; }
        public Partitionvalues partitionValues { get; set; }
        public int size { get; set; }
        public long modificationTime { get; set; }
        public bool dataChange { get; set; }
        public string stats { get; set; }
        public Tags tags { get; set; }
    }
}
