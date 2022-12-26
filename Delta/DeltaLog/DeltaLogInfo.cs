using Delta.DeltaStructure;
//using Delta.DeltaLog.Commitinfo;
//using Delta.DeltaLog.Protocol;
//using Delta.DeltaLog.Metadata;
using Delta.Common;
using System.Data;
using Delta.DeltaStructure.DeltaLog;
using System.Text.Json;

namespace Delta.DeltaLog
{
    internal class DeltaLogInfo
    {
        private readonly DeltaTable _deltaTable;
        List<Commitinfo> Commitinfo { get; set; }
        // Protocol Protocol { get; set; }
        // Metadata Metadata { get; set; }
        // List<Add> AddList { get; set; }
        // TODO List<Remove> RemoveList { get; set; }
        // TODO List<Txn> TxnList { get; set; }

        public DeltaLogInfo(DeltaTable deltaTable)
        {
            _deltaTable = deltaTable ??
                    throw new DeltaException("Explorer deltaTable is null, Delta table explorer must return a not null result.");
            Commitinfo = new List<Commitinfo>();
        }

        internal void ReadTransactionLog(DeltaTable deltaTable)
        {
            if(!CheckPointExist())
            {
                LoadLogFiles();
            }
            throw new NotImplementedException("Later!");
        }

        private void LoadLogFiles()
        {
            if (_deltaTable.DeltaLog != null)
            { 
                // TODO sort it first? how many can i expect?
                foreach(LogFile logFile in _deltaTable.DeltaLog.LogFiles)
                {
                    string dataPath = 
                        $"{_deltaTable.BasePath}{Path.DirectorySeparatorChar}{Constants.DeltaLogName}{Path.DirectorySeparatorChar}{logFile.Name}";
                    IEnumerable<string> fileLines = File.ReadLines(dataPath);
                    foreach (string line in fileLines)
                    {
                        // TODO I left it here trying to deserialise for frst time
                        Commitinfo? commitinfo =
                            JsonSerializer.Deserialize<Commitinfo>(line);
                        if (commitinfo != null)
                        {
                            Commitinfo.Add(commitinfo);
                        }
                    }
                }
            }
        }
        private bool CheckPointExist() 
            => _deltaTable.DeltaLog != null
                ? _deltaTable.DeltaLog.CheckPointFiles.Length > 0
                : throw new DeltaException("DeltaLog can't be null after being loaded by Explorer.");
    }
}
