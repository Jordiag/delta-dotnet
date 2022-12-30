using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// In the reference implementation, the provider field is used to instantiate a Spark SQL FileFormat. 
    /// As of Spark 2.4.3 there is built-in FileFormat support for parquet, csv, orc, json, and text.
    /// </summary>
    public class Format : IAction
    {
        /// <summary>
        /// Name of the encoding for files in this table
        /// </summary>
        [JsonPropertyName("provider")]
        [JsonRequired]
        #pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string Provider { get; set; }
        #pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// A map containing configuration options for the format
        /// </summary>
        [JsonPropertyName("options")]
        public Dictionary<string, string>? Options { get; set; }
    }
}
