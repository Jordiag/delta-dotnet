using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Delta.Storage.Contracts;

namespace Delta.Storage
{
    /// <summary>
    /// <inheritdoc />
    /// </summary>
    public class AzureDataLakeDir : IDeltaDirectoryInfo
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
        public AzureDataLakeDir()
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
            => DirPath = path;

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
            DirPath = path;
        }

        private DataLakeFileSystemClient _dataLakeFileSystemClient;

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

        private void GetDataLakeServiceClient(ref DataLakeServiceClient dataLakeServiceClient,
            string accountName, string clientID, string clientSecret, string tenantID, string fileSystemName)
        {

            TokenCredential credential = new ClientSecretCredential(
                tenantID, clientID, clientSecret, new TokenCredentialOptions());

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), credential);
            _dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
        }

        private void GetDataLakeServiceClient(ref DataLakeServiceClient dataLakeServiceClient,
            string accountName, string accountKey, string fileSystemName)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            dataLakeServiceClient = new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);
            _dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
        }

        private async Task<string> DownloadFileAsync(DataLakeFileSystemClient fileSystemClient, string directoryName, string fileName)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(directoryName);

            DataLakeFileClient fileClient =
                directoryClient.GetFileClient(fileName);

            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();

            var reader = new BinaryReader(downloadResponse.Value.Content);

            return reader.ReadString();
        }

        private async Task<List<PathItem>> ListFilesInDirectoryAsync(DataLakeFileSystemClient fileSystemClient, string directoryName)
        {
            var itemList = new List<PathItem>();
            IAsyncEnumerator<PathItem> enumerator =
                fileSystemClient.GetPathsAsync(directoryName).GetAsyncEnumerator();

            await enumerator.MoveNextAsync();

            PathItem item = enumerator.Current;

            while(item != null)
            {
                itemList.Add(item);

                if(!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }

            return itemList;
        }
    }
}
