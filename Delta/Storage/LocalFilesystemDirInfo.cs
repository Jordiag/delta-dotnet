using System.IO;
using Delta.Storage.Contracts;

namespace Delta.Storage
{
    /// <summary>
    /// <inheritdoc />
    /// </summary>
    public class LocalFilesystemDirInfo : IDeltaDirectoryInfo
    {
        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string? Name { get; private set; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string? FullName { get; private set; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string? DirPath { get; private set; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        /// <param name="path"><inheritdoc /></param>
        public void Set(string path) => DirPath = path;

        public void Set(string name, string fullName, string path)
        {
            Name = name;
            FullName = fullName;
            DirPath = path;
        } 

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        /// <returns><inheritdoc /></returns>
        public IDeltaDirectoryInfo[] GetDirectories()
        {
            var directoryInfo = new DirectoryInfo(DirPath);
            DirectoryInfo[] directoryInfos = directoryInfo.GetDirectories();
            var deltaDirectoryInfos = new IDeltaDirectoryInfo[directoryInfos.Length];
            for(int i = 0; i < directoryInfos.Length; i++)
            {
                var deltaDirectoryInfo = new LocalFilesystemDirInfo();
                deltaDirectoryInfo.Set(directoryInfos[i].Name, directoryInfos[i].FullName, $"{DirPath}{Path.DirectorySeparatorChar}{directoryInfos[i].Name}");
                deltaDirectoryInfos[i] = deltaDirectoryInfo;
            }

            // TODO check if partition files are not loaded,move out from explorer cross so method

            return deltaDirectoryInfos;
        }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        /// <param name="filter"><inheritdoc /></param>
        /// <returns><inheritdoc /></returns>
        public IDeltaFileInfo[] GetFiles(string filter)
        {
            var directoryInfo = new DirectoryInfo(DirPath);
            FileInfo[] fileInfos = directoryInfo.GetFiles(filter);
            var deltaFileInfos = new IDeltaFileInfo[fileInfos.Length];
            for(int i = 0; i < fileInfos.Length; i++)
            {
                var deltaFileInfo = new DeltaFileInfo(fileInfos[i].Extension, fileInfos[i].Length, fileInfos[i].Name, fileInfos[i].FullName);
                deltaFileInfos[i] = deltaFileInfo;
            }

            return deltaFileInfos;
        }
    }
}
