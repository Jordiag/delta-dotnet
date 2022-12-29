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