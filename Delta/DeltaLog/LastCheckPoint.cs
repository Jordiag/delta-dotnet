using System.Text.Json.Serialization;

namespace Delta.DeltaLog
{
    internal class LastCheckPoint
    {
        /// <summary>
        /// The version of the table when the last checkpoint was made.
        /// </summary>
        [JsonPropertyName("version")]
        [JsonRequired]
        public long Version { get; set; }
        /// <summary>
        /// The number of actions that are stored in the checkpoint.
        /// </summary>
        [JsonPropertyName("size")]
        [JsonRequired]
        public long Size { get; set; }

        /// <summary>
        /// The number of fragments if the last checkpoint was written in multiple parts. This field is optional.
        /// </summary>
        [JsonPropertyName("parts")]
        public long Parts { get; set; }
        /// <summary>
        /// The number of bytes of the checkpoint. This field is optional.
        /// </summary>
        [JsonPropertyName("sizeInBytes")]
        public long SizeInBytes { get; set; }
        /// <summary>
        /// The number of AddFile actions in the checkpoint. This field is optional.
        /// </summary>
        [JsonPropertyName("numOfAddFiles")]
        public long NumOfAddFiles { get; set; }
        /// <summary>
        /// The schema of the checkpoint file.This field is optional.
        /// </summary>
        [JsonPropertyName("checkpointSchema")]
        public string? CheckpointSchema { get; set; }

        /// <summary>
        /// The checksum of the last checkpoint JSON. This field is optional.
        /// </summary>
        [JsonPropertyName("checksum")]
        public string? Checksum { get; set; }
    }
}
