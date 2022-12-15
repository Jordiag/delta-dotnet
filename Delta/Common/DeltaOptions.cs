namespace Delta.Common
{
   public class DeltaOptions
   {
      public bool StrictTableParsing { get; }
      public bool StrictDeltaLogParsing { get; }
      public bool StrictRootFolderParsing { get; }

      public DeltaOptions(bool strictTableParsing = false, bool strictDeltaLogParsing = false, bool strictRootFolderParsing = false)
      {
         StrictTableParsing = strictTableParsing;
         StrictDeltaLogParsing = strictDeltaLogParsing;
         StrictRootFolderParsing = strictRootFolderParsing;
      }
   }
}

