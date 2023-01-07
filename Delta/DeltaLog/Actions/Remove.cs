using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// The remove action are used to modify the data in a table by removing individual logical files respectively.
    /// </summary>
    public class Remove : IAction
    {
        /// <summary>
        /// A relative path to a file from the root of the table or an absolute path to a file that should be removed from the table. 
        /// The path is a URI as specified by RFC 2396 URI Generic Syntax, which needs to be decoded to get the data file path.
        /// </summary>
        [JsonPropertyName("path")]
        [JsonRequired]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string Path { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// The time the deletion occurred, represented as milliseconds since the epoch.
        /// </summary>
        [JsonPropertyName("deletionTimestamp")]
        public long DeletionTimestamp { get; set; }

        /// <summary>
        /// When false the records in the removed file must be contained in one or more add file actions in the same version.
        /// </summary>
        [JsonPropertyName("dataChange")]
        [JsonRequired]
        public bool DataChange { get; set; }

        /// <summary>
        /// When true the fields partitionValues, size, and tags are present.
        /// </summary>
        [JsonPropertyName("extendedFileMetadata")]
        public bool ExtendedFileMetadata { get; set; }

        /// <summary>
        /// A map from partition column to value for this logical file.
        /// </summary>
        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        /// <summary>
        /// The size of this data file in bytes.
        /// </summary>
        [JsonPropertyName("size")]
        public int Size { get; set; }

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
