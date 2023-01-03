using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// The add action are used to modify the data in a table by adding  individual logical files respectively.
    /// </summary>
    public class Add : IAction
    {
        /// <summary>
        /// A relative path to a data file from the root of the table or an absolute path to a file that should be added to the table.
        /// The path is a URI as specified by RFC 2396 URI Generic Syntax, which needs to be decoded to get the data file path.
        /// </summary>
        [JsonPropertyName("path")]
        [JsonRequired]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string Path { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// A map from partition column to value for this logical file.
        /// </summary>
        [JsonPropertyName("partitionValues")]
        [JsonRequired]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public Dictionary<string, string> PartitionValues { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// The size of this data file in bytes.
        /// </summary>
        [JsonPropertyName("size")]
        [JsonRequired]
        public int Size { get; set; }

        /// <summary>
        /// The time this logical file was created, as milliseconds since the epoch.
        /// </summary>
        [JsonPropertyName("modificationTime")]
        [JsonRequired]
        public long ModificationTime { get; set; }

        /// <summary>
        /// When false the logical file must already be present in the table or the records in the added file must be contained in one or more remove actions 
        /// in the same version.
        /// </summary>
        [JsonPropertyName("dataChange")]
        [JsonRequired]
        public bool DataChange { get; set; }

        /// <summary>
        /// Contains statistics (e.g., count, min/max values for columns) about the data in this logical file.
        /// </summary>
        [JsonPropertyName("stats")]
        public string? Stats { get; set; }

        /// <summary>
        /// Map containing metadata about this logical file.
        /// </summary>
        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }

        /// <summary>
        /// Either null (or absent in JSON) when no DV is associated with this data file, 
        /// or a struct (described below) that contains necessary information about the DV that is part of this logical file.
        /// </summary>
        [JsonPropertyName("deletionVector")]
        public string? DeletionVector { get; set; }
    }
}
