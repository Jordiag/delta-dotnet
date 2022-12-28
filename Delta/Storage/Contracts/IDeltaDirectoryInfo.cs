namespace Delta.Storage.Contracts
{
    /// <summary>
    /// 
    /// Delta Directory information.
    /// </summary>
    public interface IDeltaDirectoryInfo
    {
        /// <summary>
        /// Directory Name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Directory full name.
        /// </summary>
        string FullName { get; }

        /// <summary>
        /// Directory path.
        /// </summary>
        string DirPath { get; }

        void Set(string path);

        void Set(string name, string fullName, string path);

        /// <summary>
        /// Get inner directories.
        /// </summary>
        /// <returns>Returns an array of the inner directories.</returns>
        IDeltaDirectoryInfo[] GetDirectories();


        /// <summary>
        /// Get inner files.
        /// </summary>
        /// <param name="filter">Extension filter.</param>
        /// <returns>Returns Array of files.</returns>
        IDeltaFileInfo[] GetFiles(string filter);
    }
}