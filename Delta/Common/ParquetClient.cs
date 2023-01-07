using Delta.DeltaLog.Actions;
using Parquet;
using Parquet.Data.Rows;

namespace Delta.Common
{
    /// <summary>
    /// Parquet Client to read and write parquet files.
    /// </summary>
    public class ParquetClient
    {
        /// <summary>
        /// Empty constructor.
        /// </summary>
        protected ParquetClient()
        {
        }

        /// <summary>
        /// Reads a checkpoint parquet file.
        /// </summary>
        /// <param name="fileStream">Checkpoint parquet file stream.</param>
        /// <param name="deltaOptions"></param>
        /// <param name="fileName">Checkpoint parquet file name.</param>
        /// <returns></returns>
        public static async Task<SortedList<int, IAction>> ReadCheckPointAsync(Stream? fileStream, DeltaOptions deltaOptions, string fileName)
        {
            CheckFileStream(fileStream);

            var checkPointSortedList = new SortedList<int, IAction>();
            Table table = await ParquetReader.ReadTableFromStreamAsync(fileStream);

            for(int i = 0; i < table.Count; i++)
            {

                string row = table[i].ToString().Replace('\'', '"');
                IAction? action = default;
                CheckpointRow? checkpointRow = JsonSerialiser.Deserialise<CheckpointRow>(row, fileName, deltaOptions);
                EliminateNulls(checkpointRow);

                action = DeserialiseAction<Add>(checkpointRow?.Add, deltaOptions, fileName) ?? action;
                action = DeserialiseAction<Txn>(checkpointRow?.Txn, deltaOptions, fileName) ?? action;
                action = DeserialiseAction<Remove>(checkpointRow?.Remove, deltaOptions, fileName) ?? action;
                action = DeserialiseAction<CommitInfo>(checkpointRow?.CommitInfo, deltaOptions, fileName) ?? action;
                action = DeserialiseAction<Metadata>(checkpointRow?.Metadata, deltaOptions, fileName) ?? action;
                action = DeserialiseAction<Protocol>(checkpointRow?.Protocol, deltaOptions, fileName) ?? action;
                if(action != null)
                {
                    checkPointSortedList.Add(i, action);
                }
                else
                {
                    throw new DeltaException("Action is null when this must be impossible deserialising checkpoint ones.");
                }
            }

            return checkPointSortedList;
        }

        private static void CheckFileStream(Stream? fileStream)
        {
            if(fileStream == null || !fileStream.CanRead)
            {
                throw new ArgumentNullException($"FileStream is null?: {fileStream == null}, " +
                    $"or not readable?: {fileStream?.CanRead}, when trying to start reading parquet file.");
            }
        }

        private static T? DeserialiseAction<T>(object? actionObj, DeltaOptions deltaOptions, string fileName)
        {
            var action = default(T);
            if(actionObj == null)
            {
                return action;
            }

            string? line = actionObj?.ToString();
            if(!string.IsNullOrEmpty(line))
            {
                action = JsonSerialiser.Deserialise<T>(line, fileName, deltaOptions);
            }

            return action;
        }

        private static void EliminateNulls(CheckpointRow? checkpointRow)
        {
            if(checkpointRow == null)
                return;

            bool? isNullable = checkpointRow.Add?.ToString()?.Contains("\"path\": null,");
            checkpointRow.Add = AmendAction(isNullable, checkpointRow.Add);
            isNullable = checkpointRow.Remove?.ToString()?.Contains("\"path\": null,");
            checkpointRow.Remove = AmendAction(isNullable, checkpointRow.Remove);
            isNullable = checkpointRow.CommitInfo?.ToString()?.Contains("\"timestamp\": null,");
            checkpointRow.CommitInfo = AmendAction(isNullable, checkpointRow.CommitInfo);
            isNullable = checkpointRow.Protocol?.ToString()?.Contains("minReaderVersion\": null,");
            checkpointRow.Protocol = AmendAction(isNullable, checkpointRow.Protocol);
            isNullable = checkpointRow.Metadata?.ToString()?.Contains("\"id\": null,");
            checkpointRow.Metadata = AmendAction(isNullable, checkpointRow.Metadata);
            isNullable = checkpointRow.Txn?.ToString()?.Contains("\"appId\": null,");
            checkpointRow.Txn = AmendAction(isNullable, checkpointRow.Txn);
        }

        private static object? AmendAction(bool? isNullable, object? checkpointRowAction)
            => isNullable == null || isNullable.Value ? null : checkpointRowAction;
    }
}
