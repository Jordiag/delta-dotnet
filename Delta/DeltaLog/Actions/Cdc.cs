using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// The cdc action is used to add a file containing only the data that was changed as part of the transaction. 
    /// When change data readers encounter a cdc action in a particular Delta table version, 
    /// they must read the changes made in that version exclusively using the cdc files. 
    /// If a version has no cdc action, then the data in add and remove actions are read as inserted and deleted rows, respectively.
    /// </summary>
    public class Cdc
    {
        /// <summary>
        /// A relative path to a change data file from the root of the table or an absolute path to a change data file that should be added to the table. 
        /// The path is a URI as specified by RFC 2396 URI Generic Syntax, which needs to be decoded to get the file path.
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
        /// Should always be set to false for cdc actions because they do not change the underlying data of the table.
        /// </summary>
        [JsonPropertyName("dataChange")]
        [JsonRequired]
        public bool DataChange { get; set; }

        /// <summary>
        /// Map containing metadata about this file.
        /// </summary>
        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }
    }
}
