using System.Text.Json.Serialization;

namespace Delta.TransactionLog.Metadata
{
    public class Metadata
    {
        [JsonPropertyName("id")]
        [JsonRequiredAttribute]
        public Guid Id { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("description")]
        public string Description { get; set; }

        [JsonPropertyName("format")]
        [JsonRequiredAttribute]
        public Format Format { get; set; }

        [JsonPropertyName("schemaString")]
        [JsonRequiredAttribute]
        public string SchemaString { get; set; }

        [JsonPropertyName("partitionColumns")]
        [JsonRequiredAttribute]
        public string[] PartitionColumns { get; set; }

        [JsonPropertyName("createdTime")]
        public long createdTime { get; set; }

        [JsonPropertyName("configuration")]
        [JsonRequiredAttribute]
        public Dictionary<string, string> Configuration { get; set; }
    }

}
