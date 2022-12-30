using Delta.Common;

namespace Delta.Storage
{
    /// <summary>
    /// <inheritdoc />
    /// </summary>
    public class FileSystem
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        protected FileSystem()
        { }

        /// <summary>
        /// Get a filestream from a filesystem file path.
        /// </summary>
        /// <param name="path">Filesystem file path</param>
        /// <param name="stream">Container stream</param>
        /// <returns></returns>
        /// <exception cref="DeltaException"></exception>
        public static void GetFileStream(string path, ref Stream? stream)
        {
            try
            {
                stream = System.IO.File.OpenRead(path);
            }
            catch(ArgumentException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(UnauthorizedAccessException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(PathTooLongException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(DirectoryNotFoundException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(FileNotFoundException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(ObjectDisposedException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
            catch(NotSupportedException ex)
            {
                throw new DeltaException("Get file stream failed.", ex);
            }
        }
    }
}