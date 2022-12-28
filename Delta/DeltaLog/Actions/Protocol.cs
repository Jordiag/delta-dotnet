using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// The protocol action is used to increase the version of the Delta protocol that is required to read or write a given table. Protocol versioning allows a 
    /// newer client to exclude older readers and/or writers that are missing features required to correctly interpret the transaction log. 
    /// The protocol version will be increased whenever non-forward-compatible changes are made to this specification. In the case where a client is 
    /// running an invalid protocol version, an error should be thrown instructing the user to upgrade to a newer protocol version of their Delta client library.
    /// Since breaking changes must be accompanied by an increase in the protocol version recorded in a table, clients can assume that 
    /// unrecognized fields or actions are never required in order to correctly interpret the transaction log.
    /// </summary>
    /// 
    public class Protocol
    {
        /// <summary>
        /// The minimum version of the Delta read protocol that a client must implement in order to correctly read this table.
        /// </summary>
        [JsonPropertyName("minReaderVersion")]
        [JsonRequired]
        public int MinReaderVersion { get; set; }

        /// <summary>
        /// The minimum version of the Delta write protocol that a client must implement in order to correctly write this table.
        /// </summary>
        [JsonPropertyName("minWriterVersion")]
        [JsonRequired]
        public int MinWriterVersion { get; set; }
    }
}