using System.Text.Json;
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
        /// <param name="options">Json serializer pptions.</param>
        /// <param name="fileName">Checkpoint parquet file name.   </param>
        /// <returns></returns>
        public static async Task<SortedList<int, IAction>> ReadCheckPointAsync(Stream? fileStream, JsonSerializerOptions options, string fileName)
        {
            CheckFileStream(fileStream);

            SortedList<int, IAction> checkPointSortedList = new SortedList<int, IAction>();
            Table table = await ParquetReader.ReadTableFromStreamAsync(fileStream);

            for(int i = 0; i < table.Count; i++)
            {

                string row = table[i].ToString().Replace('\'', '"');
                IAction? action = default;
                CheckpointRow? checkpointRow = Deserialises<CheckpointRow>(row, options, fileName);
                EliminateNulls(checkpointRow);

                action = DeserialiseAction<Add>(checkpointRow?.Add, options, fileName) ?? action;
                action = DeserialiseAction<Txn>(checkpointRow?.Txn, options, fileName) ?? action;
                action = DeserialiseAction<Remove>(checkpointRow?.Remove, options, fileName) ?? action;
                action = DeserialiseAction<CommitInfo>(checkpointRow?.CommitInfo, options, fileName) ?? action;
                action = DeserialiseAction<Metadata>(checkpointRow?.Metadata, options, fileName) ?? action;
                action = DeserialiseAction<Protocol>(checkpointRow?.Protocol, options, fileName) ?? action;
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

        private static T? Deserialises<T>(string line, JsonSerializerOptions options, string fileName)
        {
            try
            {
                T? action = JsonSerializer.Deserialize<T>(line, options);
                return action;
            }
            catch(ArgumentNullException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
            catch(NotSupportedException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
            catch(JsonException ex)
            {
                throw new DeltaException($"Failed to deserialise this line: {line} from this file {fileName}", ex);
            }
        }

        private static T? DeserialiseAction<T>(object? actionObj, JsonSerializerOptions options, string fileName)
        {
            var action = default(T);
            if(actionObj == null)
            {
                return action;
            }

            string? line = actionObj?.ToString();
            if(!string.IsNullOrEmpty(line))
            {
                action = Deserialises<T>(line, options, fileName);
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
