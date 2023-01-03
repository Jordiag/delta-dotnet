using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// The metaData action changes the current metadata of the table.
    /// The first version of a table must contain a metaData action.
    /// Subsequent metaData actions completely overwrite the current metadata of the table.
    /// There can be at most one metadata action in a given version of the table.
    /// Every metadata action must include required fields at a minimum.
    /// </summary>
    public class Metadata : IAction
    {
        /// <summary>
        /// Unique identifier for this table.
        /// </summary>
        [JsonPropertyName("id")]
        [JsonRequired]
        public Guid Id { get; set; }

        /// <summary>
        /// User-provided identifier for this table.
        /// </summary>
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        /// <summary>
        /// User-provided description for this table.
        /// </summary>
        [JsonPropertyName("description")]
        public string? Description { get; set; }

        /// <summary>
        /// Specification of the encoding for the files stored in the table
        /// </summary>
        [JsonPropertyName("format")]
        [JsonRequired]
        #pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public Format Format { get; set; }
        #pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// Schema of the table
        /// </summary>
        [JsonPropertyName("schemaString")]
        [JsonRequired]
        #pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string SchemaString { get; set; }
        #pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// An array containing the names of columns by which the data should be partitioned.
        /// </summary>
        [JsonPropertyName("partitionColumns")]
        public string[]? PartitionColumns { get; set; }


        /// <summary>
        /// The time when this metadata action is created, in milliseconds since the Unix epoch.
        /// </summary>
        [JsonPropertyName("createdTime")]
        public long? CreatedTime { get; set; }

        /// <summary>
        /// A map containing configuration options for the metadata action.
        /// </summary>
        [JsonPropertyName("configuration")]
        public Dictionary<string, string>? Configuration { get; set; }
    }

}
