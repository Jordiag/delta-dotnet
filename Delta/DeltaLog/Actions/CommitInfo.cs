using System.Text.Json.Serialization;

namespace Delta.DeltaLog.Actions
{
    /// <summary>
    /// CommitInfo is created (using apply and empty utilities) when:
    /// DeltaHistoryManager is requested for version and commit history(for DeltaTable.history operator and DESCRIBE HISTORY SQL command)
    /// OptimisticTransactionImpl is requested to commit(with spark.databricks.delta.commitInfo.enabled configuration property enabled)
    /// DeltaCommand is requested to commitLarge(for ConvertToDeltaCommand command and FileAlreadyExistsException was thrown)
    /// CommitInfo is used as a part of OptimisticTransactionImpl and CommitStats.
    /// </summary>
    public class CommitInfo : IAction
    {
        /// <summary>
        /// CommitInfo Timestamp in Unix time.
        /// </summary>
        [JsonRequired]
        [JsonPropertyName("timestamp")]
        public long Timestamp { get; set; }

        /// <summary>
        /// Operation is an abstraction of operations that can be executed on a Delta table.
        /// Operation is described by a name and parameters(that are simply used to create a CommitInfo for OptimisticTransactionImpl when committed and, as a way to bypass a transaction, ConvertToDeltaCommand).
        /// Operation may have performance metrics.
        /// </summary>
        [JsonRequired]
        [JsonPropertyName("operation")]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string Operation { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// Parameters of the operation (for example, predicates.)
        /// </summary>
        [JsonRequired]
        [JsonPropertyName("operationParameters")]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public Dictionary<string, string> OperationParameters { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// CommitInfo can be given extra engineInfo identifier (when created) for the engine that made the commit.
        /// Supposed to be: [JsonRequired]
        /// </summary>
        [JsonPropertyName("engineInfo")]
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string EngineInfo { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        /// <summary>
        /// Transaction ID.
        /// Supposed to be: [JsonRequired]
        /// </summary>
        [JsonPropertyName("txnId")]
        public Guid TxnId { get; set; }

        /// <summary>
        /// User ID.
        /// </summary>
        [JsonPropertyName("userId")]
        public string? UserId { get; set; }

        /// <summary>
        /// User name.
        /// </summary>
        [JsonPropertyName("userName")]
        public string? UserName { get; set; }

        /// <summary>
        /// Job info.
        /// </summary>
        [JsonPropertyName("job")]
        public Dictionary<string, string>? Job { get; set; }

        /// <summary>
        /// Notebook.
        /// </summary>
        [JsonPropertyName("notebook")]
        public Dictionary<string, string>? Notebook { get; set; }

        /// <summary>
        /// Cluster ID.
        /// </summary>
        [JsonPropertyName("clusterId")]
        public string? ClusterId { get; set; }

        /// <summary>
        /// Read Version.
        /// </summary>
        [JsonPropertyName("readVersion")]
        public long ReadVersion { get; set; }

        /// <summary>
        /// Isolatio nLevel.
        /// </summary>
        [JsonPropertyName("isolationLevel")]
        public string? IsolationLevel { get; set; }

        /// <summary>
        /// CommitInfo is given isBlindAppend flag (when created) to indicate whether a commit has blindly appended data without caring about existing files.
        /// isBlindAppend flag is used while checking for logical conflicts with concurrent updates(at commit).
        /// isBlindAppend flag is always false when DeltaCommand is requested to commitLarge.
        /// </summary>
        [JsonPropertyName("isBlindAppend")]
        public bool? IsBlindAppend { get; set; }

        /// <summary>
        /// Operation Metrics.
        /// </summary>
        [JsonPropertyName("operationMetrics")]
        public Dictionary<string, string>? OperationMetrics { get; set; }
    }
}
