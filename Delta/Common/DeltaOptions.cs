namespace Delta.Common
{
   public class DeltaOptions
   {
      public bool StrictTableParsing { get; }
      public bool StrictDeltaLogParsing { get; }
      public bool StrictRootDirectoryParsing { get; }
      public bool LockAllFiles { get; }

      public DeltaOptions(bool strictTableParsing = false, bool strictDeltaLogParsing = false, bool strictRootDirectoryParsing = false, bool lockAllFiles = false)
      {
         StrictTableParsing = strictTableParsing;
         StrictDeltaLogParsing = strictDeltaLogParsing;
         StrictRootDirectoryParsing = strictRootDirectoryParsing;
         LockAllFiles = lockAllFiles;
      }
   }
}

