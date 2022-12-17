namespace Delta.Table
{
   internal class LogFolder
   {
      internal LogCrcFile[] LogCrcFiles;
      internal LogFile[] LogFiles;
      internal CheckPointFile[] CheckPointFiles;
      internal LastCheckPointFile LastCheckPointFiles;

      internal LogFolder(LogCrcFile[] logCrcFiles, LogFile[] logFiles, CheckPointFile[] checkPointFiles, LastCheckPointFile lastCheckPointFiles)
      {
         LogCrcFiles = logCrcFiles;
         LogFiles = logFiles;
         CheckPointFiles = checkPointFiles;
         LastCheckPointFiles = lastCheckPointFiles;
      }
   }
}
