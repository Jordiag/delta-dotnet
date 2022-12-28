using Delta.Storage.Contracts;

namespace Delta.Storage
{
    /// <summary>
    /// Delta file information.
    /// </summary>
    public class DeltaFileInfo : IDeltaFileInfo
    {
        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string Extension { get; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public long Length { get; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string FullName { get; }

        /// <summary>
        /// Delta file contructior
        /// </summary>
        /// <param name="extension"><inheritdoc /></param>
        /// <param name="length"><inheritdoc /></param>
        /// <param name="name"><inheritdoc /></param>
        /// <param name="fullName"><inheritdoc /></param>
        public DeltaFileInfo(string extension, long length, string name, string fullName)
        {
            Extension = extension;
            Length = length;
            Name = name;
            FullName = fullName;
        }
    }
}