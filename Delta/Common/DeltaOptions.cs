namespace Delta.Common
{
    /// <summary>
    /// Options to use reading and writing Delta Lake table.
    /// </summary>
    public class DeltaOptions
    {
        /// <summary>
        /// Throw exceptions on code warnings while parsing Delta Lake data.
        /// </summary>
        public bool StrictTableParsing { get; }

        /// <summary>
        /// Throw exceptions on code warnings while parsing Delta log.
        /// </summary>
        public bool StrictDeltaLogParsing { get; }

        /// <summary>
        /// Throw exceptions on code warnings while parsing Delta log.
        /// </summary>
        public bool StrictRootDirectoryParsing { get; }

        /// <summary>
        /// Lock all files at initial exploration.
        /// </summary>
        public bool LockAllFiles { get; }

        /// <summary>
        /// Deserialise Delta Log json properties Ignoring case.
        /// </summary>
        public bool DeserialiseCaseInsensitive { get; }

        /// <summary>
        /// Create instance of DeltaOptions.
        /// </summary>
        public DeltaOptions(bool strictTableParsing = false, bool strictDeltaLogParsing = false, bool strictRootDirectoryParsing = false, bool lockAllFiles = false, bool deserialiseIgnoringCase = false)
        {
            StrictTableParsing = strictTableParsing;
            StrictDeltaLogParsing = strictDeltaLogParsing;
            StrictRootDirectoryParsing = strictRootDirectoryParsing;
            LockAllFiles = lockAllFiles;
            DeserialiseCaseInsensitive = deserialiseIgnoringCase;
        }
    }
}

