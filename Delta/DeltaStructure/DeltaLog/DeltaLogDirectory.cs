using Delta.DeltaStructure.Common;

namespace Delta.DeltaStructure.DeltaLog
{
    internal class DeltaLogDirectory
    {
        internal LogCrcFile[] LogCrcFiles;
        internal LogFile[] LogFiles;
        internal CheckPointFile[] CheckPointFiles;
        internal LastCheckPointFile? LastCheckPointFile;
        internal List<IgnoredFile> IgnoredFileList;

        internal DeltaLogDirectory(LogCrcFile[] logCrcFiles, LogFile[] logFiles, CheckPointFile[] checkPointFiles, LastCheckPointFile? lastCheckPointFile, List<IgnoredFile> ignoredFileList)
        {
            LogCrcFiles = logCrcFiles;
            LogFiles = logFiles;
            CheckPointFiles = checkPointFiles;
            LastCheckPointFile = lastCheckPointFile;
            IgnoredFileList = ignoredFileList;
        }
    }
}
