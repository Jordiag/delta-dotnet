using System.Text.Json.Serialization;
using Delta.DeltaLog.Actions;

namespace Delta.Common
{
    public class CheckpointRow
    {
        [JsonPropertyName("txn")]
        public object Txn { get; set; }

        [JsonPropertyName("add")]
        public object Add { get; set; }

        [JsonPropertyName("remove")]
        public object Remove { get; set; }

        [JsonPropertyName("metadata")]
        public object Metadata { get; set; }

        [JsonPropertyName("protocol")]
        public object Protocol { get; set; }

        [JsonPropertyName("commitInfo")]
        public object CommitInfo { get; set; }
    }
}
