using System.Net.Http.Headers;
using System.Text.Json.Serialization;
using Delta.DeltaLog;
using Delta.DeltaLog.Actions;
using Parquet;
using Parquet.Data;
using Thrift;

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileStream"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<CheckPoint> ReadCheckPointAsync(Stream? fileStream)
        {
            CheckFileStream(fileStream);

            CheckPoint checkPoint = new CheckPoint();
            var idsList = new List<string[]>();
            // open parquet file reader
            using(ParquetReader parquetReader = await ParquetReader.CreateAsync(fileStream))
            {
                // get file schema (available straight after opening parquet reader)
                // however, get only data fields as only they contain data values
                DataField[] dataFields = parquetReader.Schema.GetDataFields();
                // enumerate through row groups in this file
                for(int i = 0; i < parquetReader.RowGroupCount; i++)
                {
                    // create row group reader
                    using(ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                    {
                        // read all columns inside each row group (you have an option to read only
                        // required columns if you need to.
                        var columns = new DataColumn[dataFields.Length];
                        IAction? lastAction = null;
                        IAction? currentAction = null;
                        var ActionList = new List<IAction>();
                        
                        for(int c = 0; c < columns.Length; c++)
                        {
                            DataColumn currentDataColumn = await groupReader.ReadColumnAsync(dataFields[c]);
                            if(lastAction == null)
                            {
                                lastAction = GetAction(currentDataColumn);
                                currentAction = LoadAction(currentDataColumn, lastAction, currentAction);
                            }
                            else
                            {
                                if(SameAction(currentDataColumn, lastAction))
                                {
                                    currentAction = LoadAction(currentDataColumn, lastAction, currentAction);
                                }
                                else
                                {
                                    ActionList.Add(currentAction);
                                    LoadCheckPoint(currentAction, ActionList, checkPoint);
                                    ActionList.Clear();
                                    lastAction = GetAction(currentDataColumn);
                                    currentAction = LoadAction(currentDataColumn, lastAction, currentAction);
                                }
                                columns[c] = currentDataColumn;
                            }

                        }

                        // get first column, for instance
                        DataColumn firstColumn = columns[0];

                        // .Data member contains a typed array of column data you can cast to the type of the column
                        Array data = firstColumn.Data;
                        string[] ids = (string[])data;
                        idsList.Add(ids);
                    }
                }
                return checkPoint;
            }
        }

        private static CheckPoint LoadCheckPoint(IAction lastAction, IEnumerable<IAction> actionList, CheckPoint checkPoint)
        {
            string typeName = lastAction.GetType().ToString().Split('.').Last();
            switch(typeName)
            {
                case nameof(Add):
                    checkPoint.Adds = actionList.OfType<Add>().ToArray();
                    return checkPoint;
                case nameof(Txn):
                    checkPoint.Txns = actionList.OfType<Txn>().ToArray();
                    return checkPoint;
                case nameof(Remove):
                    checkPoint.Removes = actionList.OfType<Remove>().ToArray();
                    return checkPoint;
                case nameof(CommitInfo):
                    checkPoint.CommitInfos = actionList.OfType<CommitInfo>().ToArray();
                    return checkPoint;
                case nameof(Metadata):
                    checkPoint.Metadatas = actionList.OfType<Metadata>().ToArray();
                    return checkPoint;
                case nameof(Protocol):
                    checkPoint.Protocols = actionList.OfType<Protocol>().ToArray();
                    return checkPoint;
                case nameof(Cdc):
                    checkPoint.Cdcs = actionList.OfType<Cdc>().ToArray();
                    return checkPoint;
                default:
                    throw new DeltaException("Action not recognised.");
            }
        }

        private static IAction LoadAction(DataColumn currentDataColumn, IAction lastAction, IAction currentAction)
        {
            string actionText = currentDataColumn.Field.Path.Split('.')[0];
            string propertyText = currentDataColumn.Field.Name;
            actionText = $"{char.ToUpper(actionText[0])}{actionText[1..]}";

            switch(actionText)
            {
                case nameof(Add):
                    Add add = (Add)currentAction ?? new Add();
                    add.Path = propertyText == "path" ? currentDataColumn.Field.Path.Split('.')[1] : ((Add)lastAction).Path;
                    var partitionValues = new Dictionary<string, string>(); // not loaded
                    add.PartitionValues = propertyText == "partitionValues" ? partitionValues : ((Add)lastAction).PartitionValues;
                    add.Size = propertyText == "size" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).Size;
                    add.ModificationTime = propertyText == "modificationTime" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).Size;
                    add.DataChange = propertyText == "dataChange" ? bool.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).DataChange;
                    add.Stats = propertyText == "stats" ? currentDataColumn.Field.Path.Split('.')[1] : ((Add)lastAction).Stats;
                    var tags = new Dictionary<string, string>(); // not loaded
                    add.Tags = propertyText == "tags" ? tags : ((Add)lastAction).Tags;
                    add.DeletionVector = propertyText == "deletionVector" ? currentDataColumn.Field.Path.Split('.')[1] : ((Add)lastAction).DeletionVector;
                    return add;
                case nameof(Remove):
                    Remove remove = (Remove)currentAction ?? new Remove();
                    remove.Path = propertyText == "path" ? currentDataColumn.Field.Path.Split('.')[1] : ((Add)lastAction).Path;
                    partitionValues = new Dictionary<string, string>(); // not loaded
                    remove.PartitionValues = propertyText == "partitionValues" ? partitionValues : ((Add)lastAction).PartitionValues;
                    remove.Size = propertyText == "size" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).Size;
                    remove.DeletionTimestamp = propertyText == "modificationTime" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).Size;
                    remove.DataChange = propertyText == "dataChange" ? bool.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Add)lastAction).DataChange;
                    remove.ExtendedFileMetadata = propertyText == "extendedFileMetadata" ? bool.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Remove)lastAction).ExtendedFileMetadata;
                    tags = new Dictionary<string, string>(); // not loaded
                    remove.Tags = propertyText == "tags" ? tags : ((Add)lastAction).Tags;
                    remove.DeletionVector = propertyText == "deletionVector" ? currentDataColumn.Field.Path.Split('.')[1] : ((Add)lastAction).DeletionVector;
                    return remove;
                case nameof(Txn):
                    Txn txn = (Txn)currentAction ?? new Txn();
                    txn.AppId = propertyText == "appId" ? new Guid(currentDataColumn?.Data?.GetValue(2)?.ToString()) : ((Txn)lastAction).AppId;
                    txn.Version = propertyText == "version" ? long.Parse(currentDataColumn?.Data?.GetValue(2)?.ToString()) : ((Txn)lastAction).Version;
                    txn.LastUpdated = propertyText == "lastUpdated" ? long.Parse(currentDataColumn?.Data?.GetValue(2)?.ToString()) : ((Txn)lastAction).LastUpdated;
                    return txn;
                case nameof(CommitInfo):
                    var commitInfo = new CommitInfo();
                    commitInfo.Timestamp = propertyText == "modificationTime" ? long.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((CommitInfo)lastAction).Timestamp;
                    commitInfo.Operation = propertyText == "operation" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).Operation;
                    var operationParameters = new Dictionary<string, string>(); // not loaded
                    commitInfo.OperationParameters = propertyText == "path" ? operationParameters : ((CommitInfo)lastAction).OperationParameters;
                    commitInfo.EngineInfo = propertyText == "engineInfo" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).EngineInfo;
                    commitInfo.TxnId = propertyText == "txnId" ? new Guid(currentDataColumn.Field.Path.Split('.')[1]) : ((CommitInfo)lastAction).TxnId;
                    commitInfo.UserId = propertyText == "userId" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).UserId;
                    commitInfo.UserName = propertyText == "userName" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).UserName;
                    var job = new Dictionary<string, string>(); // not loaded
                    commitInfo.Job = propertyText == "job" ? job : ((CommitInfo)lastAction).Job;
                    var notebook = new Dictionary<string, string>(); // not loaded
                    commitInfo.Notebook = propertyText == "notebook" ? notebook : ((CommitInfo)lastAction).Notebook;
                    commitInfo.ClusterId = propertyText == "clusterId" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).ClusterId;
                    commitInfo.ReadVersion = propertyText == "readVersion" ? long.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((CommitInfo)lastAction).ReadVersion;
                    commitInfo.IsolationLevel = propertyText == "isolationLevel" ? currentDataColumn.Field.Path.Split('.')[1] : ((CommitInfo)lastAction).IsolationLevel;
                    commitInfo.IsBlindAppend = propertyText == "isBlindAppend" ? bool.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((CommitInfo)lastAction).IsBlindAppend;
                    var operationMetrics = new Dictionary<string, string>(); // not loaded
                    commitInfo.OperationMetrics = propertyText == "notebook" ? operationMetrics : ((CommitInfo)lastAction).OperationMetrics;
                    return commitInfo;
                case nameof(Metadata):
                    var metadata = new Metadata();
                    metadata.Id = propertyText == "id" ? new Guid(currentDataColumn.Field.Path.Split('.')[1]) : ((Metadata)lastAction).Id;
                    metadata.Name = propertyText == "name" ? currentDataColumn.Field.Path.Split('.')[1] : ((Metadata)lastAction).Name;
                    metadata.Description = propertyText == "description" ? currentDataColumn.Field.Path.Split('.')[1] : ((Metadata)lastAction).Description;
                    var format = new Format(); // not loaded
                    metadata.Format = propertyText == "format" ? format : ((Metadata)lastAction).Format;
                    metadata.SchemaString = propertyText == "schemaString" ? currentDataColumn.Field.Path.Split('.')[1] : ((Metadata)lastAction).SchemaString;
                    string[] partitionColumns = new string[1]; // not loaded
                    metadata.PartitionColumns = propertyText == "partitionColumns" ? partitionColumns : ((Metadata)lastAction).PartitionColumns;
                    metadata.CreatedTime = propertyText == "createdTime" ? long.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Metadata)lastAction).CreatedTime;
                    var configuration = new Dictionary<string, string>(); // not loaded
                    metadata.Configuration = propertyText == "configuration" ? configuration : ((Metadata)lastAction).Configuration;
                    return metadata;
                case nameof(Protocol):
                    var protocol = new Protocol();
                    protocol.MinReaderVersion = propertyText == "createdTime" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Protocol)lastAction).MinReaderVersion;
                    protocol.MinReaderVersion = propertyText == "createdTime" ? int.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Protocol)lastAction).MinReaderVersion;
                    return protocol;
                default:
                    throw new DeltaException($"This type of action: '{actionText}' can't be load while reading transactions log.");
            }
        }

        private static bool SameAction(DataColumn currentDataColumn, IAction lastAction)
        {
            string actionText = currentDataColumn.Field.Path.Split('.')[0];
            actionText = $"{char.ToUpper(actionText[0])}{actionText[1..]}";

            return actionText == lastAction.GetType().Name;
        }

        private static IAction GetAction(DataColumn currentDataColumn)
        {
            string actionText = currentDataColumn.Field.Path.Split('.')[0];
            actionText = $"{char.ToUpper(actionText[0])}{actionText[1..]}";
            switch(actionText)
            {
                case nameof(Add):
                    return new Add();
                case nameof(Txn):
                    return new Txn();
                default:
                    return new CommitInfo();
            }
        }

        public static async Task<T> ReadDataAsync<T>(Stream fileStream)
        {
            throw new NotImplementedException();
        }
    }
}


//if(result.data?.Count != lastCheckPoint.Size)
//{
//    throw new DeltaException($"Checkpoint expected rows: '{lastCheckPoint.Size}' where not found: '{result.data?.Count}', size property in {Constants.LastCheckPointName} looks corrupted.");
//}