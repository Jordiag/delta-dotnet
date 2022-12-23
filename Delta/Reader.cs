using Delta.Common;
using Delta.DeltaStructure;

namespace Delta
{
    /// <summary>
    /// Delta Lake table reader
    /// </summary>
    public class Reader
    {
        private readonly DeltaTable _deltaTable;

        /// <summary>
        /// Creates and instance of Delta Lake table Reader
        /// </summary>
        /// <param name="path">Full path to root Delta Lake table directory</param>
        /// <param name="deltaOptions"></param>
        public Reader(string path, DeltaOptions deltaOptions)
        {
            var explorer = new Explorer(path, deltaOptions);
            _deltaTable = explorer.ReadStructure();
        }
    }
}
