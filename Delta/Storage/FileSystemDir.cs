using Delta.Common;
using Delta.Storage.Contracts;

namespace Delta.Storage
{
    /// <summary>
    /// <inheritdoc />
    /// </summary>
    public class FileSystemDir : IDeltaDirectoryInfo
    {
        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string FullName { get; private set; }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string DirPath { get; private set; }

        /// <summary>
        /// Creates instance.
        /// </summary>
        public FileSystemDir()
        {
            Name = string.Empty;
            FullName = string.Empty;
            DirPath = string.Empty;
        }

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        /// <param name="path"><inheritdoc /></param>
        public void Set(string path)
            => DirPath = GetCrossSoPath(path);

        /// <summary>
        /// <inheritdoc />
        /// </summary>
        /// <param name="name"><inheritdoc /></param>
        /// <param name="fullName"><inheritdoc /></param>
        /// <param name="path"><inheritdoc /></param>
        public void Set(string name, string fullName, string path)
        {
            Name = name;
            FullName = fullName;
            DirPath = DirPath == null ? GetCrossSoPath(path) : path;
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
                var deltaDirectoryInfo = new FileSystemDir();
                deltaDirectoryInfo.Set(directoryInfos[i].Name, directoryInfos[i].FullName, $"{DirPath}{Path.DirectorySeparatorChar}{directoryInfos[i].Name}");
                deltaDirectoryInfos[i] = deltaDirectoryInfo;
            }

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

        /// <summary>
        /// Makes path cross operation system
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static string GetCrossSoPath(string path)
        {
            string[] pathArray = path.Split('/');
            string dataRelativePath = string.Join(Path.DirectorySeparatorChar, pathArray);

            return $"{dataRelativePath}";
        }


        /// <summary>
        /// Get a filestream from a filesystem file path.
        /// </summary>
        /// <param name="path">Filesystem file path</param>
        /// <returns></returns>
        /// <exception cref="DeltaException"></exception>
        public static async Task<Stream> GetFileStreamAsync(string path)
        {
            Stream? fileStream = null;
            try
            {
                fileStream = System.IO.File.OpenRead(path);

                return fileStream;
            }
            catch(System.ArgumentException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(System.UnauthorizedAccessException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(System.IO.PathTooLongException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(System.IO.DirectoryNotFoundException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(System.IO.FileNotFoundException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(System.ObjectDisposedException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }

            finally
            {
                if(fileStream != null)
                {
                    await fileStream.DisposeAsync();
                }
            }
        }
    }
}