namespace Delta.Table
{
   internal class DeltaLog
   {
      internal LogCrcFile[] LogCrcFiles;
      internal LogFile[] LogFiles;
      internal CheckPointFile[] CheckPointFiles;
      internal LastCheckPointFile? LastCheckPointFiles;
      internal List<IgnoredFile> IgnoredFileList;

      internal DeltaLog(LogCrcFile[] logCrcFiles, LogFile[] logFiles, CheckPointFile[] checkPointFiles, LastCheckPointFile? lastCheckPointFiles, List<IgnoredFile> ignoredFileList)
      {
         LogCrcFiles = logCrcFiles;
         LogFiles = logFiles;
         CheckPointFiles = checkPointFiles;
         LastCheckPointFiles = lastCheckPointFiles;
         IgnoredFileList = ignoredFileList;  
      }
   }
}
