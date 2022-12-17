namespace Delta.Common
{
   public class DeltaOptions
   {
      public bool StrictTableParsing { get; }
      public bool StrictDeltaLogParsing { get; }
      public bool StrictRootFolderParsing { get; }
      public bool LockAllFiles { get; }

      public DeltaOptions(bool strictTableParsing = false, bool strictDeltaLogParsing = false, bool strictRootFolderParsing = false, bool lockAllFiles = false)
      {
         StrictTableParsing = strictTableParsing;
         StrictDeltaLogParsing = strictDeltaLogParsing;
         StrictRootFolderParsing = strictRootFolderParsing;
         LockAllFiles = lockAllFiles;
      }
   }
}

