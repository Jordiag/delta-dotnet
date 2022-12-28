namespace Delta.Storage.Contracts
{
    /// <summary>
    /// Delta file information
    /// </summary>
    public interface IDeltaFileInfo
    {
        /// <summary>
        /// File extension.
        /// </summary>
        string Extension { get; }
        
        /// <summary>
        /// File size in bytes.
        /// </summary>
        long Length { get; }
        
        /// <summary>
        /// Fine name.
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// File full name.
        /// </summary>
        string FullName { get; }
    }
}