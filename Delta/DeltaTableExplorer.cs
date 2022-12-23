using Delta.Common;
using Delta.DeltaStructure;
using Delta.DeltaStructure.Common;
using Delta.DeltaStructure.Data;
using Delta.DeltaStructure.DeltaLog;


namespace Delta
{
    public class DeltaTableExplorer
    {
        private readonly DeltaOptions _deltaOptions;
        public DeltaTable DeltaTable { get; set; }

        public DeltaTableExplorer(string path, DeltaOptions deltaOptions)
        {
            string basePath = Path.Combine(Directory.GetCurrentDirectory(), path);
            _deltaOptions = deltaOptions;
            DeltaTable = new DeltaTable(basePath);
        }

        public void ReadDeltaFolderStructure()
        {
            WalkDeltaTree(new DirectoryInfo(DeltaTable.BasePath), DeltaTable, FolderType.Root);
        }

        private void WalkDeltaTree(DirectoryInfo directoryInfo, DeltaTable deltaTable, FolderType currentFolderType)
        {
            DirectoryInfo[] subDirectories = Array.Empty<DirectoryInfo>();

            switch(currentFolderType)
            {
                case FolderType.DeltaLog:
                    DeltaLogFolder deltaLogFolder = ProcessDeltaLog(directoryInfo);
                    DeltaTable.SetDeltaLog(deltaLogFolder);
                    break;
                case FolderType.Root:
                    (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) root =
                       ProcessRoot(directoryInfo, ref subDirectories, currentFolderType);
                    DeltaTable.SetRootData(root.dataFileList, root.crcFileList);
                    break;
                case FolderType.Partition:
                    ProcessPartitionFolder(directoryInfo, ref subDirectories, deltaTable.Partitions);
                    break;
                case FolderType.Unknown:
                    string message = $"Unknown type of folder: {directoryInfo.FullName}.";
                    AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
                    break;
                default:
                    message = $"Unknown default type of folder: {directoryInfo.FullName}.";
                    AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
                    break;
            }

            foreach(DirectoryInfo dirInfo in subDirectories)
            {
                currentFolderType = GetCurrentFolderType(dirInfo);
                WalkDeltaTree(dirInfo, deltaTable, currentFolderType);
            }
        }

        private void ProcessPartitionFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs, Partition rootPartition)
        {
            CheckSubFolders(directoryInfo);

            FileInfo[] files = GetFiles(directoryInfo);
            subDirs = directoryInfo.GetDirectories();
            Partition partition;

            if(files.Length == 0 && subDirs.Length == 0)
            {
                string message = "Partition folder can't contain 0 data files and 0 other folder.";
                AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
            }
            else if(files.Length > 0 && subDirs.Length > 0)
            {
                string message = "Partition folder can't contain > 0 data files and > 0 other folder.";
                AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
            }
            else if(files.Length == 0 && subDirs.Length > 0)
            {
                partition = GetPartition(directoryInfo);
                AddToHeirachy(directoryInfo, ref partition, rootPartition);
            }
            else if(files.Length > 0 && subDirs.Length == 0)
            {
                (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) dataFolderResult = ProcessRootDataFiles(directoryInfo);
                partition = GetPartitionData(directoryInfo, dataFolderResult.dataFileList, dataFolderResult.crcFileList);
                AddToHeirachy(directoryInfo, ref partition, rootPartition);
            }
            else
            {
                string message = $"Partition folder can't be processed. Files length: {files.Length} SubDirs Length: {subDirs.Length}";
                AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
            }

        }

        private Partition GetPartition(DirectoryInfo directoryInfo)
        {
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            string parent = GetParentPath(directoryInfo, DeltaTable.BasePath);

            return new Partition(parent, key, value);
        }

        private Partition GetPartitionData(DirectoryInfo directoryInfo, DataFile[] dataFiles, DataCrcFile[] dataCrcFiles)
        {
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            string parent = GetParentPath(directoryInfo, DeltaTable.BasePath);

            return new PartitionData(parent, key, value, dataFiles, dataCrcFiles);
        }

        private static string GetParentName(string fullPath)
        {
            string[] fullPathArray = fullPath.Split(Path.DirectorySeparatorChar);
            string parentName = fullPathArray[^2];

            return parentName;
        }

        private void AddToHeirachy(DirectoryInfo directoryInfo, ref Partition partition, Partition rootPartition)
        {
            if(rootPartition == null)
            {
                rootPartition = new Partition(DeltaTable.BasePath);
                rootPartition.PartitionList.Add(partition);
            }
            else
            {
                string parentName = GetParentName(directoryInfo.FullName);
                Partition? parent = GetParent(rootPartition, parentName);

                if(parent == null)
                {
                    rootPartition.PartitionList.Add(partition);
                }
                else
                {
                    parent?.PartitionList.Add(partition);
                }
            }
        }

        private Partition? GetParent(Partition folder, string parentName)
        {
            if(folder == null)
            {
                return null;
            }
            if(parentName == $"{folder.Key}={folder.Value}")
            {
                return folder;
            }
            foreach(Partition currentPartition in folder.PartitionList)
            {
                Partition? found = GetParent(currentPartition, parentName);
                if(found != null)
                {
                    return found;
                }
            }

            return null;
        }

        private static string GetParentPath(DirectoryInfo directoryInfo, string basePath)
        {
            int startPosition = directoryInfo.FullName.IndexOf(basePath);
            string path = directoryInfo.FullName[startPosition..];
            path = path[..^directoryInfo.Name.Length];

            return path;

        }

        private (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) ProcessRoot(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs, FolderType currentFolderType)
        {
            (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) result = ProcessRootDataFiles(directoryInfo);
            subDirs = directoryInfo.GetDirectories();
            bool noUnderScoreFolderExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

            if(currentFolderType == FolderType.Root && result.dataFileList.Length > 1 && noUnderScoreFolderExist)
            {
                string message = "Error file extension not recognised as root folder file.";
                AddToIgnoreList(message, directoryInfo, DeltaTable.IgnoredFolderList);
                return (Array.Empty<DataFile>(), Array.Empty<DataCrcFile>(), new List<IgnoredFile>());
            }
            else
            {
                return result;
            }
        }

        private static FolderType GetCurrentFolderType(DirectoryInfo dirInfo)
        {
            return dirInfo.Name switch
            {
                Constants.DeltaLogName => FolderType.DeltaLog,
                Constants.DeltaIndexName => FolderType.DeltaIndex,
                Constants.ChangeDataName => FolderType.ChangeData,
                _ => IsPartitionFolder(dirInfo.Name) ? FolderType.Partition : FolderType.Unknown,
            };
        }

        private static bool IsPartitionFolder(string name)
        {
            return name.Contains('=') && name.Split('=').Length == 2;
        }

        private (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) ProcessRootDataFiles(DirectoryInfo directoryInfo)
        {
            FileInfo[] files = GetFiles(directoryInfo);
            var dataFileList = new DataFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            var crcFileList = new DataCrcFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
            var ignoredFileList = new List<IgnoredFile>();

            if(files != null)
            {
                uint dataFileCounter = 0;
                uint crcFilecounter = 0;
                for(int counter = 0; counter < files.Length; counter++)
                {
                    FileInfo file = files[counter];
                    (bool isIgnored, long index, Guid guid, CompressionType compressionType) fileInfo = GetFileInfo(file, ignoredFileList);

                    if (fileInfo.isIgnored)
                    {
                        return (dataFileList, crcFileList, ignoredFileList);
                    }

                    switch(file.Extension)
                    {
                        case Constants.CrcExtension:
                            var crcFile = new DataCrcFile(fileInfo.index, fileInfo.guid.ToString(), file.Length, file.Name);
                            crcFileList[dataFileCounter] = crcFile;
                            dataFileCounter++;
                            break;
                        case Constants.ParquetExtension:
                            var dataFile = new DataFile(fileInfo.index, fileInfo.guid.ToString(), fileInfo.compressionType, file.Length, file.Name);
                            dataFileList[crcFilecounter] = dataFile;
                            crcFilecounter++;
                            break;
                        default:
                            string message = "Error file extension not recognised as root folder file.";
                            AddToIgnoreList(message, file, ignoredFileList);
                            break;
                    }
                }
            }

            return (dataFileList, crcFileList, ignoredFileList);
        }

        private void AddToIgnoreList(string message, FileInfo file, List<IgnoredFile> ignoredFileList)
        {
            if(_deltaOptions.StrictRootFolderParsing)
            {
                throw new DeltaException(message);
            }
            else
            {
                IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
                ignoredFileList.Add(ignoredFile);
            }
        }

        private void AddToIgnoreList(string message, DirectoryInfo directoryInfo, List<IgnoredFolder> ignoredFolderList)
        {
            if(_deltaOptions.StrictRootFolderParsing)
            {
                throw new DeltaException(message);
            }
            else
            {
                IgnoredFolder ignoredFolder = new IgnoredFolder(directoryInfo.FullName);
                ignoredFolderList.Add(ignoredFolder);
            }
        }

        private static FileInfo[] GetFiles(DirectoryInfo directoryInfo)
        {
            FileInfo[] files;
            try
            {
                files = directoryInfo.GetFiles("*.*");
            }
            catch(UnauthorizedAccessException e)
            {
                throw new DeltaException("Error trying to get files regarding unauthorised access.", e);
            }

            catch(DirectoryNotFoundException e)
            {
                throw new DeltaException("Error trying to get files regarding directory not found.", e);
            }

            return files;
        }

        private DeltaLogFolder ProcessDeltaLog(DirectoryInfo directoryInfo)
        {
            CheckSubFolders(directoryInfo);

            FileInfo[] files = GetFiles(directoryInfo);
            var logCrcFiles = new LogCrcFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            var logFiles = new LogFile[files.Count(file => file.Extension == Constants.JsonExtension)];
            var checkPointFiles = new CheckPointFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
            var ignoredFileList = new List<IgnoredFile>();
            LastCheckPointFile? lastCheckPointFile = null;

            if(files != null)
            {
                uint logCrcFileCounter = 0;
                uint logFileCounter = 0;
                uint checkPointFileCounter = 0;

                for(int counter = 0; counter < files.Length; counter++)
                {
                    FileInfo file = files[counter];
                    switch(file.Extension)
                    {
                        case Constants.JsonExtension:
                            ProcessLogFile(file, logFiles, ref logFileCounter, ignoredFileList);
                            break;
                        case Constants.CrcExtension:
                            ProcessCrcFile(files, file, logCrcFiles, ref logCrcFileCounter, ignoredFileList);
                            break;
                        case Constants.ParquetExtension:
                            ProcessParquetFile(file, checkPointFiles, ref checkPointFileCounter, ignoredFileList);
                            break;
                        default:
                            ProcessDefaultFile(files, file, ref lastCheckPointFile, ignoredFileList);
                            break;
                    }
                }
            }
            var logFolder = new DeltaLogFolder(logCrcFiles, logFiles, checkPointFiles, lastCheckPointFile, ignoredFileList);

            return logFolder;
        }

        private void CheckSubFolders(DirectoryInfo directoryInfo)
        {
            DirectoryInfo[] directories = directoryInfo.GetDirectories();
            if(directories.Length > 0 && _deltaOptions.StrictRootFolderParsing)
            {
                throw new DeltaException("Delta_log folder has subfolders.");
            }
        }

        private void ProcessLogFile(FileInfo file, LogFile[] logFiles, ref uint logFileCounter, List<IgnoredFile> ignoredFileList)
        {
            string fileNameWithoutExtension = file.Name[..^Constants.JsonExtension.Length];
            bool isNumbers = Constants.onlyNumbersRegex.IsMatch(fileNameWithoutExtension);
            if(isNumbers)
            {
                long index = ParseFileIndex(fileNameWithoutExtension, "File {file} has a non parseable umber to long.");
                var logFile = new LogFile(index, file.Name);
                logFiles[logFileCounter] = logFile;
                logFileCounter++;
            }
            else
            {
                string message = "File name is not only numbers.";
                AddToIgnoreList(message, file, ignoredFileList);
            }
        }

        private void ProcessCrcFile(FileInfo[] files, FileInfo file, LogCrcFile[] logCrcFiles, ref uint logCrcFileCounter, List<IgnoredFile> ignoredFileList)
        {
            IEnumerable<FileInfo> matches = files.Where(f => f.Name == file.Name);
            if(matches.Count() == 1)
            {
                var logCrcFile = new LogCrcFile(file.Name);
                logCrcFiles[logCrcFileCounter] = logCrcFile;
                logCrcFileCounter++;
            }
            else
            {
                string message = !matches.Any() ?
                    $"Crc file '{file.Name}' has no associated json file in delta_log folder." :
                    $"Crc file '{file.Name}' has more than 1 associated json file in delta_log folder.";
                AddToIgnoreList(message, file, ignoredFileList);
            }


        }

        private void ProcessParquetFile(FileInfo file, CheckPointFile[] checkPointFiles, ref uint checkPointFileCounter, List<IgnoredFile> ignoredFileList)
        {
            string fileNameWithoutExtension = file.Name.Substring(0, file.Name.Length - Constants.ParquetExtension.Length - Constants.CheckPointExtension.Length);

            bool isNumbers = Constants.onlyNumbersRegex.IsMatch(fileNameWithoutExtension);
            if(file.Name.EndsWith($"{Constants.CheckPointExtension}{Constants.ParquetExtension}") && isNumbers)
            {
                var checkPoint = new CheckPointFile(long.Parse(fileNameWithoutExtension), file.Name);
                checkPointFiles[checkPointFileCounter] = checkPoint;
                checkPointFileCounter++;
            }
            else
            {
                string message = "File extension not recognised as root folder file or name is not only numbers.";
                AddToIgnoreList(message, file, ignoredFileList);
            }
        }

        private void ProcessDefaultFile(FileInfo[] files, FileInfo file, ref LastCheckPointFile? lastCheckPointFile, List<IgnoredFile> ignoredFileList)
        {
            if(file.Name == Constants.LastCheckPointName)
            {
                int LastCheckPointFileNumber = files.Count(file => file.Name == Constants.LastCheckPointName);
                if(LastCheckPointFileNumber > 1)
                {
                    throw new DeltaException($"Only one '{Constants.LastCheckPointName}' is allowed.");
                }
                lastCheckPointFile = new LastCheckPointFile(file.Name);

                return;
            }
            string message = "Error file extension not recognised as root folder file.";
            AddToIgnoreList(message, file, ignoredFileList);
        }

        private (bool isIgnored, long index, Guid guid, CompressionType compressionType) GetFileInfo(FileInfo file, List<IgnoredFile> ignoredFileList)
        {
            string name = file.Name.Substring(0, file.Name.Length - file.Extension.Length);
            string[] namePointParts = name.Split('.');
            int infoIndex = string.IsNullOrEmpty(namePointParts[0]) ? 1 : 0;
            string[] nameMinusParts = namePointParts[infoIndex].Split('-');

            if(nameMinusParts[0] == Constants.PartText)
            {
                string parseErrrorMessage = "This file {file} has a non parseable index number to long.";
                return namePointParts.Length switch
                {
                    // TODO still fails, test with multiple test DATA folders
                    1 => (false, ParseFileIndex(nameMinusParts[1], parseErrrorMessage), ExtractGuid(nameMinusParts), CompressionType.Uncompressed),
                    2 => (false, ParseFileIndex(nameMinusParts[1], parseErrrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1])),
                    3 => (false, ParseFileIndex(nameMinusParts[1], parseErrrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
                    4 => (false, ParseFileIndex(nameMinusParts[1], parseErrrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
                    5 => (false, ParseFileIndex(nameMinusParts[1], parseErrrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[3])),
                    _ => throw new DeltaException($"This parquet part file '{name}' has more dots than expected."),
                };
            }
            else
            {
                string message = $"This parquet file '{file.Name}' must start with `part`.";
                AddToIgnoreList(message, file, ignoredFileList);
                return (true, 0, Guid.Empty, CompressionType.Uncompressed);
            }
        }

        private static long ParseFileIndex(string fileIndexText, string message)
        {
            bool parseOk = long.TryParse(fileIndexText, out long index);
            return !parseOk ? throw new DeltaException(message) : index;
        }

        private static Guid ExtractGuid(string[] nameParts)
        {
            string guidString = $"{nameParts[2]}-{nameParts[3]}-{nameParts[4]}-{nameParts[5]}-{nameParts[6]}";
            string parseErrorMessage = $"Error parsing this guid string {guidString}";
            bool parseOk = Guid.TryParse(guidString, out Guid guid);
            return !parseOk ? throw new DeltaException(parseErrorMessage) : guid;
        }

        private static CompressionType ExtractCompression(string compressionText) => compressionText switch
        {
            Constants.SnappyText => CompressionType.Snappy,
            _ => throw new DeltaException($"compression not yet supported in this nuget library: '{compressionText}'."),
        };
    }
}
