using System.Text.Json;
using Delta.Common;
using Delta.DeltaLog.Actions;
using Delta.DeltaStructure;
using Delta.DeltaStructure.DeltaLog;

namespace Delta.DeltaLog
{
    internal class DeltaLogInfo
    {
        private readonly DeltaTable _deltaTable;
        private readonly DeltaOptions _deltaOptions;
        internal Protocol? Protocol { get; private set; }
        internal Metadata? Metadata { get; private set; }
        internal List<CommitInfo> CommitinfoList { get; private set; }
        internal List<Add> AddList { get; private set; }
        internal List<Remove> RemoveList { get; private set; }
        internal List<Txn> TxnList { get; private set; }

        public DeltaLogInfo(DeltaTable deltaTable, DeltaOptions deltaOptions)
        {
            _deltaTable = deltaTable ??
                    throw new DeltaException("Explorer deltaTable is null, Delta table explorer must return a not null result.");
            _deltaOptions = deltaOptions;
            CommitinfoList = new List<CommitInfo>();
            AddList = new List<Add>();
            RemoveList = new List<Remove>();
            TxnList = new List<Txn>();
        }

        internal void ReadTransactionLog(DeltaTable deltaTable)
        {
            if(!CheckPointExist())
            {
            LoadLogActions();
            }
            throw new NotImplementedException("Later!");
        }

        private void LoadLogActions()
        {
            if(_deltaTable.DeltaLog != null)
            {
                foreach(LogFile logFile in _deltaTable.DeltaLog.LogFiles)
                {
                    string dataPath =
                        $"{_deltaTable.BasePath}{Path.DirectorySeparatorChar}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{logFile.Name}";
                    IEnumerable<string> fileLines = File.ReadLines(dataPath);
                    foreach(string line in fileLines)
                    {
                        (ActionType actionType, string line) action = GetAction(line, logFile);
                        switch(action.actionType)
                        {
                            case ActionType.Protocol:
                                Protocol? protocol =
                                    JsonSerializer.Deserialize<Protocol>(action.line, GetJsonSerializerOptions());
                                SetAction(protocol, Protocol, logFile);
                                break;     
                            case ActionType.Add:
                                Add? add =
                                    JsonSerializer.Deserialize<Add>(action.line, GetJsonSerializerOptions());
                                AddToList(add, AddList);
                                break;
                            case ActionType.Remove:
                                Remove? remove =
                                    JsonSerializer.Deserialize<Remove>(action.line, GetJsonSerializerOptions());
                                AddToList(remove, RemoveList);
                                break;
                            case ActionType.Metadata:
                                Metadata? metadata =
                                    JsonSerializer.Deserialize<Metadata>(action.line, GetJsonSerializerOptions());
                                SetAction(metadata, Metadata, logFile);
                                break;
                            case ActionType.Txn:
                                Txn? txn =
                                    JsonSerializer.Deserialize<Txn>(action.line, GetJsonSerializerOptions());
                                AddToList(txn, TxnList);
                                break;
                            case ActionType.CommitInfo:
                                CommitInfo? commitInfo =
                                    JsonSerializer.Deserialize<CommitInfo>(action.line, GetJsonSerializerOptions());
                                AddToList(commitInfo, CommitinfoList);
                                break;
                        }
                    }
                }
            }
        }

        private static void SetAction<T>(T? action, T? property, LogFile logFile)
        {
            property = property == null
                    ? action ?? throw new DeltaException($"{nameof(property)} action parsed from Json is null. File: {logFile.Name}")
                    : throw new DeltaException($"{nameof(property)} action must be loaded once, this was the secont time it appeared, file: {logFile.Name}");
        }

        private static void AddToList<T>(T? action, List<T> list)
        {
            if(action != null)
            {
                list.Add(action);
            }
        }

        private JsonSerializerOptions GetJsonSerializerOptions()
        {
            return new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = _deltaOptions.DeserialiseCaseInsensitive,
            };
        }

        private static (ActionType actionType, string line) GetAction(string line, LogFile logFile)
        {
            ActionType actionType;
            int start = line.IndexOf('{');
            int end = line.IndexOf(':');
            string action = line.Substring(start + 1, end - start - 1).Trim().Replace("\"", "");
            switch(action)
            {
                case "protocol":
                    line = line.Substring(start + action.Length);
                    actionType = ActionType.Protocol;
                    break;
                case "add":
                    return (ActionType.Add, line);
                case "remove":
                    return (ActionType.Add, line);
                case "metadata":
                    return (ActionType.Add, line);
                case "txn":
                    return (ActionType.Add, line);
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
