using System.Linq;
using System.Linq.Expressions;
using System.Net.Mime;
using Azure.Storage.Files.DataLake.Models;
using Delta.DeltaLog;
using Delta.DeltaLog.Actions;
using Parquet;
using Parquet.Data;
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
                        IAction lastAction = null;
                        var ActionList = new List<IAction>();
                        for(int c = 0; c < columns.Length; c++)
                        {
                            DataColumn currentDataColumn = await groupReader.ReadColumnAsync(dataFields[c]);
                            if (lastAction == null){
                                lastAction = GetAction(currentDataColumn);
                                LoadAction(currentDataColumn, lastAction);
                            }
                            else
                            {
                                if (SameAction(currentDataColumn, lastAction))
                                {
                                    LoadAction(currentDataColumn, lastAction);
                                }
                                else
                                {
                                    LoadCheckPoint(lastAction, ActionList, checkPoint);
                                    ActionList.Clear();
                                    lastAction = GetAction(currentDataColumn);
                                    LoadAction(currentDataColumn, lastAction);
                                }
                                ActionList.Add(lastAction);
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
            var typeName = lastAction.GetType().ToString().Split('.').Last();
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

        private static IAction LoadAction(DataColumn currentDataColumn, IAction lastAction)
        {
            string actionText = currentDataColumn.Field.Path.Split('.')[0];
            string propertyText = currentDataColumn.Field.Name;
            actionText = $"{char.ToUpper(actionText[0])}{actionText[1..]}";

            switch(actionText)
            {
                case nameof(Add):
                    return new Add();
                case nameof(Txn):
                    //((Txn)lastAction).AppId = propertyText == "appId" ? new Guid(currentDataColumn.Field.Path.Split('.')[1]) : ((Txn)lastAction).AppId;
                    //((Txn)lastAction).Version = propertyText == "version" ? long.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Txn)lastAction).Version;
                    //((Txn)lastAction).LastUpdated = propertyText == "lastUpdated" ? long.Parse(currentDataColumn.Field.Path.Split('.')[1]) : ((Txn)lastAction).LastUpdated;
                    return lastAction;
                default:
                    return new CommitInfo();
            }

            return lastAction;
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