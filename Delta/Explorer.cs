using System.Reflection;
using Delta.Common;
using Delta.DeltaStructure;
using Delta.DeltaStructure.Common;
using Delta.DeltaStructure.Data;
using Delta.DeltaStructure.DeltaLog;


namespace Delta
{
    /// <summary>
    /// Explorer Delta Lake directory structure.
    /// </summary>
    public class Explorer
    {
        private readonly DeltaOptions _deltaOptions;
        private readonly DeltaTable _deltaTable;

        /// <summary>
        /// Constructs an explorer instance.
        /// </summary>
        /// <param name="path">Path where data root is.</param>
        /// <param name="deltaOptions">Options to read data lake directory.</param>
        public Explorer(string path, DeltaOptions deltaOptions)
        {
            string basePath = GetCrossSoFullPath(path);

            _deltaOptions = deltaOptions;
            _deltaTable = new DeltaTable(basePath);
        }

        private static string GetCrossSoFullPath(string path)
        {
            string[] pathArray = path.Split(Path.DirectorySeparatorChar);
            string dataRelativePath = string.Join(Path.DirectorySeparatorChar, pathArray);

            string assamblyLocation = Assembly.GetExecutingAssembly().Location;
            var uri = new UriBuilder(assamblyLocation);
            string tempPath = Uri.UnescapeDataString(uri.Path);
            string? fullAssamblyPath = Path.GetDirectoryName(tempPath);
            string basePath = $"{fullAssamblyPath}{dataRelativePath}";

            return basePath;
        }

        /// <summary>
        /// Red the whole Delta Lake Table folder structure.
        /// </summary>
        /// <returns></returns>
        public DeltaTable ReadStructure()
        {
            var tempPartition = new Partition(string.Empty);
            WalkDeltaTree(new DirectoryInfo(_deltaTable.BasePath), _deltaTable, DirectoryType.Root, tempPartition);
            _deltaTable.SetPartitions(tempPartition.PartitionList.ToArray());

            return _deltaTable;
        }

        private void WalkDeltaTree(DirectoryInfo directoryInfo, DeltaTable deltaTable, DirectoryType currentDirectoryType, Partition tempPartition)
        {
            DirectoryInfo[] subDirectories = Array.Empty<DirectoryInfo>();

            switch(currentDirectoryType)
            {
                case DirectoryType.DeltaLog:
                    DeltaLogDirectory deltaLogDirectory = ProcessDeltaLog(directoryInfo);
                    _deltaTable.SetDeltaLog(deltaLogDirectory);
                    break;
                case DirectoryType.Root:
                    (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) root =
                       ProcessRoot(directoryInfo, ref subDirectories, currentDirectoryType);
                    _deltaTable.SetRootData(root.dataFileList, root.crcFileList);
                    break;
                case DirectoryType.Partition:
                    ProcessPartitionDirectory(directoryInfo, ref subDirectories, tempPartition);
                    break;
                case DirectoryType.Unknown:
                    string message = $"Unknown type of directory: {directoryInfo.FullName}.";
                    AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
                    break;
                default:
                    message = $"Unknown default type of directory: {directoryInfo.FullName}.";
                    AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
                    break;
            }

            foreach(DirectoryInfo dirInfo in subDirectories)
            {
                currentDirectoryType = GetCurrentDirectoryType(dirInfo);
                WalkDeltaTree(dirInfo, deltaTable, currentDirectoryType, tempPartition);
            }
        }

        private void ProcessPartitionDirectory(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs, Partition rootPartition)
        {
            CheckSubDirectories(directoryInfo);

            FileInfo[] files = GetFiles(directoryInfo);
            subDirs = directoryInfo.GetDirectories();
            Partition partition;

            if(files.Length == 0 && subDirs.Length == 0)
            {
                string message = "Partition directory can't contain 0 data files and 0 other directory.";
                AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
            }
            else if(files.Length > 0 && subDirs.Length > 0)
            {
                string message = "Partition directory can't contain > 0 data files and > 0 other directory.";
                AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
            }
            else if(files.Length == 0 && subDirs.Length > 0)
            {
                partition = GetPartition(directoryInfo);
                AddToHeirachy(directoryInfo, ref partition, rootPartition);
            }
            else if(files.Length > 0 && subDirs.Length == 0)
            {
                (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) dataDirectoryResult = ProcessDataFiles(directoryInfo);
                partition = GetPartitionData(directoryInfo, dataDirectoryResult.dataFileList, dataDirectoryResult.crcFileList);
                AddToHeirachy(directoryInfo, ref partition, rootPartition);
            }
            else
            {
                string message = $"Partition directory can't be processed. Files length: {files.Length} SubDirectories Length: {subDirs.Length}";
                AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
            }

        }

        private Partition GetPartition(DirectoryInfo directoryInfo)
        {
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            string parent = GetParentPath(directoryInfo, _deltaTable.BasePath);

            return new Partition(parent, key, value);
        }

        private Partition GetPartitionData(DirectoryInfo directoryInfo, DataFile[] dataFiles, DataCrcFile[] dataCrcFiles)
        {
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            string parent = GetParentPath(directoryInfo, _deltaTable.BasePath);

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
                rootPartition = new Partition(_deltaTable.BasePath);
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

        private Partition? GetParent(Partition directory, string parentName)
        {
            if(directory == null)
            {
                return null;
            }
            if(parentName == $"{directory.Key}={directory.Value}")
            {
                return directory;
            }
            foreach(Partition currentPartition in directory.PartitionList)
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

        private (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) ProcessRoot(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs, DirectoryType currentDirectoryType)
        {
            (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) result = ProcessDataFiles(directoryInfo);
            subDirs = directoryInfo.GetDirectories();
            bool noUnderScoreDirectoryExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

            if(currentDirectoryType == DirectoryType.Root && result.dataFileList.Length > 1 && noUnderScoreDirectoryExist)
            {
                string message = "Error file extension not recognised as root directory file.";
                AddToIgnoreList(message, directoryInfo, _deltaTable.IgnoredDirectoryList);
                return (Array.Empty<DataFile>(), Array.Empty<DataCrcFile>(), new List<IgnoredFile>());
            }
            else
            {
                return result;
            }
        }

        private static DirectoryType GetCurrentDirectoryType(DirectoryInfo dirInfo)
        {
            return dirInfo.Name switch
            {
                Constants.DeltaLogName => DirectoryType.DeltaLog,
                Constants.DeltaIndexName => DirectoryType.DeltaIndex,
                Constants.ChangeDataName => DirectoryType.ChangeData,
                _ => IsPartitionDirectory(dirInfo.Name) ? DirectoryType.Partition : DirectoryType.Unknown,
            };
        }

        private static bool IsPartitionDirectory(string name)
        {
            return name.Contains('=') && name.Split('=').Length == 2;
        }

        private (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) ProcessDataFiles(DirectoryInfo directoryInfo)
        {
            FileInfo[] files = GetFiles(directoryInfo);
            var dataFileList = new DataFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
            var crcFileList = new DataCrcFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            var ignoredFileList = new List<IgnoredFile>();

            if(files != null)
            {
                uint dataFileCounter = 0;
                uint crcFilecounter = 0;
                for(int counter = 0; counter < files.Length; counter++)
                {
                    FileInfo file = files[counter];
                    (bool isIgnored, long index, Guid guid, CompressionType compressionType) fileInfo = GetFileInfo(file, ignoredFileList);

                    if(fileInfo.isIgnored)
                    {
                        return (dataFileList, crcFileList, ignoredFileList);
                    }

                    switch(file.Extension)
                    {
                        case Constants.CrcExtension:
                            var crcFile = new DataCrcFile(fileInfo.index, fileInfo.guid.ToString(), file.Length, file.Name);
                            crcFileList[crcFilecounter] = crcFile;
                            crcFilecounter++;
                            break;
                        case Constants.ParquetExtension:
                            var dataFile = new DataFile(fileInfo.index, fileInfo.guid.ToString(), fileInfo.compressionType, file.Length, file.Name);
                            dataFileList[dataFileCounter] = dataFile;
                            dataFileCounter++;
                            break;
                        default:
                            string message = "Error file extension not recognised as root directory file.";
                            AddToIgnoreList(message, file, ignoredFileList);
                            break;
                    }
                }
            }

            return (dataFileList, crcFileList, ignoredFileList);
        }

        private void AddToIgnoreList(string message, FileInfo file, List<IgnoredFile> ignoredFileList)
        {
            if(_deltaOptions.StrictRootDirectoryParsing)
            {
                throw new DeltaException(message);
            }
            else
            {
                IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
                ignoredFileList.Add(ignoredFile);
            }
        }

        private void AddToIgnoreList(string message, DirectoryInfo directoryInfo, List<IgnoredDirectory> ignoredDirectoryList)
        {
            if(_deltaOptions.StrictRootDirectoryParsing)
            {
                throw new DeltaException(message);
            }
            else
            {
                IgnoredDirectory ignoredDirectory = new IgnoredDirectory(directoryInfo.FullName);
                ignoredDirectoryList.Add(ignoredDirectory);
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

        private DeltaLogDirectory ProcessDeltaLog(DirectoryInfo directoryInfo)
        {
            CheckSubDirectories(directoryInfo);

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
            var logDirectory = new DeltaLogDirectory(logCrcFiles, logFiles, checkPointFiles, lastCheckPointFile, ignoredFileList);

            return logDirectory;
        }

        private void CheckSubDirectories(DirectoryInfo directoryInfo)
        {
            DirectoryInfo[] directories = directoryInfo.GetDirectories();
            if(directories.Length > 0 && _deltaOptions.StrictRootDirectoryParsing)
            {
                throw new DeltaException("Delta_log directory has subDirectories.");
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
                    $"Crc file '{file.Name}' has no associated json file in delta_log directory." :
                    $"Crc file '{file.Name}' has more than 1 associated json file in delta_log directory.";
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
                string message = "File extension not recognised as root directory file or name is not only numbers.";
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
            string message = "Error file extension not recognised as root directory file.";
            AddToIgnoreList(message, file, ignoredFileList);
        }

        private (bool isIgnored, long index, Guid guid, CompressionType compressionType) GetFileInfo(FileInfo file, List<IgnoredFile> ignoredFileList)
        {
            int initialPosition = GetInitialPosition(file.Name);
            string name = file.Name.Substring(initialPosition, file.Name.Length - file.Extension.Length - initialPosition);

            string[] namePointParts = name.Split('.');
            int infoIndex = string.IsNullOrEmpty(namePointParts[0]) ? 1 : 0;
            string[] nameMinusParts = namePointParts[infoIndex].Split('-');

            string parseErrorMessage = $"This file {file.Name} has a non parseable index number to long.";
            string dotsParseErrorMessage = $"This parquet part file '{name}' has more dot parts than expected: {namePointParts.Length}";

            if(nameMinusParts[0] == Constants.PartText && nameMinusParts.Length == 8)
            {

                return namePointParts.Length switch
                {
                    1 => (false, ParseFileIndex(nameMinusParts[1], parseErrorMessage), ExtractGuid(nameMinusParts), CompressionType.Uncompressed),
                    2 => (false, ParseFileIndex(nameMinusParts[1], parseErrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1])),
                    3 => (false, ParseFileIndex(nameMinusParts[1], parseErrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1])),
                    _ => throw new DeltaException(dotsParseErrorMessage),
                };
            }
            if(nameMinusParts[0] == Constants.PartText && nameMinusParts.Length == 7)
            {
                return namePointParts.Length switch
                {
                    3 => (false, ParseFileIndex(nameMinusParts[1], parseErrorMessage), ExtractGuid(nameMinusParts), CompressionType.Uncompressed),
                    4 => (false, ParseFileIndex(nameMinusParts[1], parseErrorMessage), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
                    _ => throw new DeltaException(dotsParseErrorMessage),
                };
            }
            else
            {
                string message = $"This parquet file '{file.Name}' must start with `part`.";
                AddToIgnoreList(message, file, ignoredFileList);
                return (true, 0, Guid.Empty, CompressionType.Uncompressed);
            }
        }

        private static int GetInitialPosition(string name)
        {
            if(name.StartsWith(".."))
                return 2;

            return name.StartsWith('.') ? 1 : 0;
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
