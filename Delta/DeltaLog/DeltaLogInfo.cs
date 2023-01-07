using System.Text.Json;
using Delta.Common;
using Delta.DeltaLog.Actions;
using Delta.DeltaStructure;
using Delta.DeltaStructure.DeltaLog;
using Delta.Storage;
using Parquet.Data.Rows;

namespace Delta.DeltaLog
{
    internal class DeltaLogInfo
    {
        private readonly DeltaTable _deltaTable;
        private readonly DeltaOptions _deltaOptions;

        internal SortedList<int, IAction> DeltaLogActionList { get; }
        internal SortedList<int, Row> AddRows { get; }
        internal SortedList<int, Row> RemoveRows { get; }

        public DeltaLogInfo(DeltaTable deltaTable, DeltaOptions deltaOptions)
        {
            _deltaTable = deltaTable;
            _deltaOptions = deltaOptions;
            DeltaLogActionList = new SortedList<int, IAction>();
            AddRows= new SortedList<int, Row>();
            RemoveRows= new SortedList<int, Row>();
        }

        internal async Task LoadDeltaLogActionsAsync()
        {
            if(!CheckPointExist())
            {
                LoadLogActions(0);
            }
            else
            {
                CheckPointFile? checkPointFile = GetCheckPointFile();
                long nextIndex = await GetActionsFromParquetfileAsync(checkPointFile);
                LoadLogActions(nextIndex);
            }
            // TODO check non repeateable actions when laoded
            // TODO actions need to set sizes and mandatories removing nullables
        }

        private async Task<long> GetActionsFromParquetfileAsync(CheckPointFile? checkpointFile)
        {
            if(checkpointFile?.Name == null)
            {
                throw new DeltaException($"Failed to find the last CheckPoint file in: {_deltaTable.BasePath}{Path.DirectorySeparatorChar}{Constants.DeltaLogName}");
            }

            Stream? stream = null;
            string checkpointPath =
                $"{_deltaTable?.BasePath}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{checkpointFile.Name}";
            try
            {
                FileSystem.GetFileStream(checkpointPath, ref stream);
                var jsonOptions = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = _deltaOptions.DeserialiseCaseInsensitive,
                };
                jsonOptions.Converters.Add(new DictionaryConverter());

                SortedList<int, IAction> checkPointActionList = await ParquetClient.ReadCheckPointAsync(stream, _deltaOptions, checkpointFile.Name);
                for(int i = checkPointActionList.First().Key; i <= checkPointActionList.Last().Key; i++)
                {
                    IAction checkPointAction = checkPointActionList[i];
                    DeltaLogActionList.Add(i, checkPointAction);
                }

                return checkpointFile.Index + 1;

            }
            catch(DeltaException ex)
            {
                throw new DeltaException($"Failed to load checkpoint data: {checkpointPath}", ex);
            }
            finally
            {
                stream?.Dispose();
            }
        }

        private CheckPointFile? GetCheckPointFile()
        {
            if(_deltaTable?.DeltaLog?.LastCheckPointFile != null)
            {
                return GetLastCheckPoint();
            }
            else
            {
                long? max = _deltaTable?.DeltaLog?.CheckPointFiles.Max(checkPoint => checkPoint.Index);
                if(max.HasValue)
                {
                    return _deltaTable?.DeltaLog?.CheckPointFiles?.FirstOrDefault(checkPoint => checkPoint.Index == max);
                }
            }

            return null;
        }

        private CheckPointFile? GetLastCheckPoint()
        {
            string? lastCheckPointName = _deltaTable?.DeltaLog?.LastCheckPointFile?.Name;
            if(_deltaTable == null || lastCheckPointName == null)
            {
                return null;
            }

            string lastCheckpointPath =
                $"{_deltaTable.BasePath}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{lastCheckPointName}";

            string line = File.ReadLines(lastCheckpointPath).ToArray()[0];

            LastCheckPoint? lastCheckPoint = JsonSerialiser.Deserialise<LastCheckPoint>(line, lastCheckpointPath, _deltaOptions);

            if(lastCheckPoint == null)
            {
                throw new DeltaException($"Failed to deserialise {Constants.LastCheckPointName} from this file {lastCheckpointPath}");
            }

            try
            {
                return _deltaTable?.DeltaLog?.CheckPointFiles?.SingleOrDefault(file => file.Index == lastCheckPoint.Version);
            }
            catch(ArgumentNullException ex)
            {
                throw new DeltaException($"Failed to find CheckPoint file with index: {lastCheckPoint.Version}", ex);
            }
            catch(InvalidOperationException ex)
            {
                throw new DeltaException($"Failed to find CheckPoint file with index: {lastCheckPoint.Version}", ex);
            }
        }

        private void LoadLogActions(long start)
        {
            if(_deltaTable.DeltaLog != null)
            {
                long last = _deltaTable.DeltaLog.LogFiles.Max(file => file.Index);
                for(long i = start; i <= last; i++)
                {
                    LogFile? logFile = _deltaTable.DeltaLog.LogFiles.FirstOrDefault(logFile => logFile.Index == i);
                    GetLogFileActions(logFile);
                }
            }
        }

        private void GetLogFileActions(LogFile? logFile)
        {
            if(logFile == null)
            {
                return;
            }

            string dataPath =
                $"{_deltaTable.BasePath}{Path.DirectorySeparatorChar}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{logFile.Name}";
            IEnumerable<string> fileLines = File.ReadLines(dataPath);

            for(int i = 0; i < fileLines.Count(); i++)
            {
                string line = fileLines.ElementAt(i);
                (ActionType actionType, string line) actionInfo = GetActionInfo(line, logFile);
                IAction? action = default;
                switch(actionInfo.actionType)
                {
                    case ActionType.Protocol:
                        action = JsonSerialiser.Deserialise<Protocol?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                    case ActionType.Add:
                        action = JsonSerialiser.Deserialise<Add?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                    case ActionType.Remove:
                        action = JsonSerialiser.Deserialise<Remove?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                    case ActionType.Metadata:
                        action = JsonSerialiser.Deserialise<Metadata?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                    case ActionType.Txn:
                        action = JsonSerialiser.Deserialise<Txn?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                    case ActionType.CommitInfo:
                        action = JsonSerialiser.Deserialise<CommitInfo?>(actionInfo.line, logFile.Name, _deltaOptions);
                        break;
                }
                AddToList(action);
            }
        }

        private void AddToList<T>(T? action)
        {
            if(action != null)
            {
                DeltaLogActionList.Add(DeltaLogActionList.Last().Key + 1, (IAction)action);
            }
        }

        private static (ActionType actionType, string line) GetActionInfo(string line, LogFile logFile)
        {
            ActionType actionType;
            int start = line.IndexOf('{');
            int end = line.IndexOf(':');
            string action = line.Substring(start + 1, end - start - 1).Trim().Replace("\"", "");
            switch(action)
            {
                case "protocol":
                    actionType = ActionType.Protocol;
                    break;
                case "add":
                    actionType = ActionType.Add;
                    break;
                case "remove":
                    actionType = ActionType.Remove;
                    break;
                case "metaData":
                    actionType = ActionType.Metadata;
                    break;
                case "txn":
                    actionType = ActionType.Txn;
                    break;
                case "commitInfo":
                    actionType = ActionType.CommitInfo;
                    break;
                default:
                    if(line.Length > 20)
                    {
                        throw new DeltaException($"Unrecognised action type: '{action}' in line: '{line[..20]}...' in logFile{logFile.Name}.");
                    }
                    else
                    {
                        throw new DeltaException($"Unrecognised action type: '{action}' in logFile{logFile.Name}.");
                    }
            }
            int last = line.LastIndexOf('}');
            start = end + 1;
            int charsToRemove = line.Length - last;
            line = line[start..^charsToRemove];

            return (actionType, line);
        }

        private bool CheckPointExist()
            => _deltaTable.DeltaLog != null
                ? _deltaTable.DeltaLog.CheckPointFiles.Length > 0
                : throw new DeltaException("DeltaLog can't be null after being loaded by Explorer.");
    }
}
