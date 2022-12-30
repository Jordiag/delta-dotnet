using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// Incremental processing systems (e.g., streaming systems) that track progress using their own application-specific versions need to record what progress 
    /// has been made, in order to avoid duplicating data in the face of failures and retries during a write. Transaction identifiers allow this information to 
    /// be recorded atomically in the transaction log of a delta table along with the other actions that modify the contents of the table.
    /// Transaction identifiers are stored in the form of appId version pairs, where appId is a unique identifier for the process that is modifying the table 
    /// and version is an indication of how much progress has been made by that application.The atomic recording of this information along with modifications to 
    /// the table enables these external system to make their writes into a Delta table idempotent.
    /// </summary>
    public class Txn : IAction
    {
        /// <summary>
        /// A unique identifier for the application performing the transaction
        /// </summary>
        [JsonPropertyName("appId")]
        [JsonRequired]
        public Guid AppId { get; set; }

        /// <summary>
        /// An application-specific numeric identifier for this transaction
        /// </summary>
        [JsonPropertyName("version")]
        [JsonRequired]
        public long Version { get; set; }

        /// <summary>
        /// The time when this transaction action is created, in milliseconds since the Unix epoch
        /// </summary>
        [JsonPropertyName("lastUpdated")]
        public long LastUpdated { get; set; }
    }
}
