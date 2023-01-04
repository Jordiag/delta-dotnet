﻿using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
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

        internal async Task ReadTransactionLogAsync()
        {
            if(!CheckPointExist())
            {
                LoadLogActions();
            }
            else
            {
                SortedList<int, IAction>? deltaLogActions = await LoadLogActionsWithCheckpointAsync();
            }
        }

        /// <summary>
        /// Loads all log actions considering checkpoint if exist.
        /// ASSUMPTION: _last_checkpoint file is considered optional.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="DeltaException"></exception>
        private async Task<SortedList<int, IAction>?> LoadLogActionsWithCheckpointAsync()
        {
            if(_deltaTable == null || _deltaTable.DeltaLog == null)
            {
                throw new DeltaException("Explorer deltaTable is null or deltaTable.DeltaLog is null.");
            }

            CheckPointFile? checkPointFile = GetCheckPointFile();
            string?  checkpointFileName = checkPointFile?.Name;
            long?  checkpointFileIndex = checkPointFile?.Index;

            SortedList<int, IAction>?  sortedActionsList = await GetCheckPointActionsAsync(checkpointFileName);
            return await ReadJsonLogFilesAsync(sortedActionsList, checkpointFileIndex);
        }

        private async Task<SortedList<int, IAction>?> GetCheckPointActionsAsync(string? checkpointFileName)
        {
            if(checkpointFileName == null)
            {
                throw new DeltaException($"Failed to find the last CheckPoint file in: {_deltaTable.BasePath}{Path.DirectorySeparatorChar}{Constants.DeltaLogName}");
            }

            return await GetActionsFromParquetfileAsync(checkpointFileName);
        }

        private async Task<SortedList<int, IAction>> GetActionsFromParquetfileAsync(string checkpointFileName)
        {
            Stream? stream = null;
            string checkpointPath =
                $"{_deltaTable?.BasePath}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{checkpointFileName}";
            try
            {
                FileSystem.GetFileStream(checkpointPath, ref stream);
                var jsonOptions = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = _deltaOptions.DeserialiseCaseInsensitive,
                };
                jsonOptions.Converters.Add(new DictionaryJsonConverter());

                return await ParquetClient.ReadCheckPointAsync(stream, jsonOptions, checkpointFileName);
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
                if(max != null)
                {
                    return _deltaTable?.DeltaLog?.CheckPointFiles?.FirstOrDefault(checkPoint => checkPoint.Index == max);
                }
            }

            return null;
        }

        private CheckPointFile? GetLastCheckPoint()
        {
            string? lastCheckPointName = _deltaTable?.DeltaLog?.LastCheckPointFile?.Name;
            if (_deltaTable == null || lastCheckPointName == null)
            {
                return null;
            }

            string lastCheckpointPath =
                $"{_deltaTable.BasePath}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{lastCheckPointName}";

            string line = File.ReadLines(lastCheckpointPath).ToArray()[0];

            LastCheckPoint? lastCheckPoint = Deserialise<LastCheckPoint>(line, GetJsonSerializerOptions(), lastCheckpointPath);

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

        private async Task<SortedList<int, IAction>?> ReadJsonLogFilesAsync(SortedList<int, IAction>? sortedActionsList, long? checkpointFileIndex)
        {
            int? start = sortedActionsList?.Count;
            long? max = _deltaTable?.DeltaLog?.CheckPointFiles.Max(checkPoint => checkPoint.Index);
            for(long? i = checkpointFileIndex + 1; i < max; i++)
            {
                LogFile? currentLogFile = _deltaTable?.DeltaLog?.LogFiles.FirstOrDefault(file => file.Index == i);
                if(currentLogFile != null && sortedActionsList != null)
                {
                    SortedList<int, IAction> actionSortedList = await GetActionsFromParquetfileAsync(currentLogFile.Name);
                    if (actionSortedList.Count > 0 && start != null)
                    {
                        for(int c = 0; c < actionSortedList.Count; c++)
                        {
                            sortedActionsList.Add(start.Value, actionSortedList[c]);
                        }

                    }
                }
            }

            return sortedActionsList;
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
                                Protocol? protocol = Deserialise<Protocol?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                Protocol = GetAction(protocol, Protocol, logFile);
                                break;
                            case ActionType.Add:
                                Add? add = Deserialise<Add?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                AddToList(add, AddList);
                                break;
                            case ActionType.Remove:
                                Remove? remove = Deserialise<Remove?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                AddToList(remove, RemoveList);
                                break;
                            case ActionType.Metadata:
                                Metadata? metadata = Deserialise<Metadata?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                Metadata = GetAction(metadata, Metadata, logFile);
                                break;
                            case ActionType.Txn:
                                Txn? txn = Deserialise<Txn?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                AddToList(txn, TxnList);
                                break;
                            case ActionType.CommitInfo:
                                CommitInfo? commitInfo = Deserialise<CommitInfo?>(action.line, GetJsonSerializerOptions(), logFile.Name);
                                AddToList(commitInfo, CommitinfoList);
                                break;
                        }
                    }
                }
            }
        }

        private static T? Deserialise<T>(string line, JsonSerializerOptions options, string fileName)
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

        private static T? GetAction<T>(T? action, T? property, LogFile logFile)
        {
            return property == null
                    ? action ?? throw new DeltaException($"{nameof(property)} action parsed from Json is null. File: {logFile.Name}")
                    : throw new DeltaException($"{nameof(property)} action must be loaded once, this was the secont time it appeared, file: {logFile.Name}");
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
