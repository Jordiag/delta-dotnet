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

        private static List<T> DeserialiseAction<T>(object? actionObj, JsonSerializerOptions options, string fileName)
        {
            var list = new List<T>();

            var action = default(T);

            try
            {
                string? line = actionObj?.ToString();
                if(!string.IsNullOrEmpty(line))
                {
                    action = Deserialises<T>(line, options, fileName);
                }
                if(action != null)
                {
                    list.Add(action);
                }
                return list;
            }
            catch(DeltaException)
            {
                return list;
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
        public static async Task<CheckPoint> ReadCheckPointAsync(Stream? fileStream, JsonSerializerOptions options, string fileName)
        {
            CheckFileStream(fileStream);

            CheckPoint checkPoint = new CheckPoint();
            Table table = await ParquetReader.ReadTableFromStreamAsync(fileStream);

            for(int i = 0; i < table.Count; i++)
            {
                string row = table[i].ToString().Replace('\'', '"');

                CheckpointRow? checkpointRow = Deserialises<CheckpointRow>(row, options, fileName);

                List<Add> addList = DeserialiseAction<Add>(checkpointRow?.Add, options, fileName);
                checkPoint.Adds = AddItems(checkPoint.Adds, addList);
                List<Txn> txnList = DeserialiseAction<Txn>(checkpointRow?.Txn, options, fileName);
                checkPoint.Txns = AddItems(checkPoint.Txns, txnList);
                List<Remove> removeList = DeserialiseAction<Remove>(checkpointRow?.Remove, options, fileName);
                checkPoint.Removes = AddItems(checkPoint.Removes, removeList);
                List<CommitInfo> commitInfoList = DeserialiseAction<CommitInfo>(checkpointRow?.CommitInfo, options, fileName);
                checkPoint.CommitInfos = AddItems(checkPoint.CommitInfos, commitInfoList);
                List<Metadata> metadataList = DeserialiseAction<Metadata>(checkpointRow?.Metadata, options, fileName);
                checkPoint.Metadatas = AddItems(checkPoint.Metadatas, metadataList);
                List<Protocol> protocolList = DeserialiseAction<Protocol>(checkpointRow?.Protocol, options, fileName);
                checkPoint.Protocols = AddItems(checkPoint.Protocols, protocolList);
            }

            return checkPoint;
        }
    }
}
