namespace DeltaLake.Research.App.Example1.Metadata
{
    public class Metadata
    {
        public string id { get; set; }
        public Format format { get; set; }
        public string schemaString { get; set; }
        public string[] partitionColumns { get; set; }
        public Configuration configuration { get; set; }
        public long createdTime { get; set; }
    }

}
