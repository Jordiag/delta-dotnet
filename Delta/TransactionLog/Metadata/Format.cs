using System.Text.Json.Serialization;

namespace Delta.TransactionLog.Metadata
{
    public class Format
    {
        [JsonPropertyName("provider")]
        public string Provider { get; set; }

        [JsonPropertyName("options")]
        public Dictionary<string,string> Options { get; set; }
    }

}
