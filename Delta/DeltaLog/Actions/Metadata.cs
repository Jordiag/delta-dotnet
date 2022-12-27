using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    public class Metadata
    {
        [JsonPropertyName("id")]
        [JsonRequired]
        public Guid Id { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("description")]
        public string Description { get; set; }

        [JsonPropertyName("format")]
        [JsonRequired]
        public Format Format { get; set; }

        [JsonPropertyName("schemaString")]
        [JsonRequired]
        public string SchemaString { get; set; }

        [JsonPropertyName("partitionColumns")]
        [JsonRequired]
        public string[] PartitionColumns { get; set; }

        [JsonPropertyName("createdTime")]
        public long createdTime { get; set; }

        [JsonPropertyName("configuration")]
        [JsonRequired]
        public Dictionary<string, string> Configuration { get; set; }
    }

}
