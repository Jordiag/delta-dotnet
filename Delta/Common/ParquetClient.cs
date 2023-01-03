using System;
using System.Text.Json;
using Delta.DeltaLog;
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

            try
            {
                string? line = actionObj?.ToString();
                if(!string.IsNullOrEmpty(line))
                {
                    action = Deserialises<T>(line, options, fileName);
                }

                return action;
            }
            catch(DeltaException)
            {
                return action;
            }
        }

        private static List<T> AddItems<T>(List<T> actionlist, List<T> toAddList)
        {
            if(toAddList.Count > 0)
            {
                actionlist.AddRange(toAddList);
            }

            return actionlist;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileStream"></param>
        /// <param name="options"></param>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public static async Task<SortedList<int, IAction>> ReadCheckPointAsync(Stream? fileStream, JsonSerializerOptions options, string fileName)
        {
            CheckFileStream(fileStream);

            SortedList<int, IAction> checkPointSortedList = new SortedList<int, IAction>();
            Table table = await ParquetReader.ReadTableFromStreamAsync(fileStream);

            for(int i = 0; i < table.Count; i++)
            {
                // TODO skip nulled values
                string row = table[i].ToString().Replace('\'', '"');
                IAction? action = default;
                CheckpointRow? checkpointRow = Deserialises<CheckpointRow>(row, options, fileName);

                action = DeserialiseAction<Add>(checkpointRow?.Add, options, fileName) ?? action;
                action = DeserialiseAction<Txn>(checkpointRow?.Txn, options, fileName) ?? action;
                action = DeserialiseAction<Remove>(checkpointRow?.Remove, options, fileName) ?? action;
                action = DeserialiseAction<CommitInfo>(checkpointRow?.CommitInfo, options, fileName) ?? action;
                action = DeserialiseAction<Metadata>(checkpointRow?.Metadata, options, fileName) ?? action;
                action = DeserialiseAction<Protocol>(checkpointRow?.Protocol, options, fileName) ?? action;
                if (action != null)
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
    }
}
