using Delta.Common;
using Delta.Table;

namespace Delta
{
   public class DeltaTableExplorer
   {
      private readonly DeltaOptions _deltaOptions;
      public DeltaTable DeltaTable { get; set; }

      public DeltaTableExplorer(string basePath, DeltaOptions deltaOptions)
      {
         _deltaOptions = deltaOptions;
         DeltaTable = new DeltaTable(basePath);
      }
      // TODO are all files and folder ignored captured?

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
               Table.DeltaLog deltaLogResult = ProcessDeltaLog(directoryInfo);
               DeltaTable.LoadDeltaLog(deltaLogResult);
               break;
            case FolderType.Root:
               (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) root =
                  ProcessRoot(directoryInfo, ref subDirectories, currentFolderType);
               DeltaTable.LoadRootDataTable(root.dataFileList, root.crcFileList, root.IgnoredFileList);
               break;
            case FolderType.Partition:
               ProcessPartitionFolder(directoryInfo, ref subDirectories, deltaTable.Partitions);
               break;
            case FolderType.Unknown:
               if(!_deltaOptions.StrictTableParsing)
               {
                  var ignoredFolder = new IgnoredFolder(directoryInfo.FullName);
                  DeltaTable.AddIgnoredFolder(ignoredFolder);
               }
               throw new DeltaException($"Unknown type of folder: {directoryInfo.FullName}.");
            default:
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
         FileInfo[] files = GetFiles(directoryInfo);
         subDirs = directoryInfo.GetDirectories();
         Partition partition;

         if(files.Length == 0 && subDirs.Length == 0)
         {
            throw new DeltaException("Partition folder can't contain 0 data files and 0 other folder.");
         }
         else if(files.Length > 0 && subDirs.Length > 0)
         {
            throw new DeltaException("Partition folder can't contain > 0 data files and > 0 other folder.");
         }
         else if(files.Length == 0 && subDirs.Length > 0)
         {
            partition = GetPartition(directoryInfo, Array.Empty<DataFile>(), Array.Empty<DataCrcFile>());
         }
         else if(files.Length > 0 && subDirs.Length == 0)
         {
            (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) dataFolderResult = ProcessRootDataFiles(directoryInfo);
            partition = GetPartition(directoryInfo, dataFolderResult.dataFileList, dataFolderResult.crcFileList);
         }
         else
         {
            throw new DeltaException($"Partition folder can't be prcessed. Files length: {files.Length} SubDirs Length: {subDirs.Length}");
         }
         AddToHeirachy(directoryInfo, ref partition, rootPartition);
      }

      private Partition GetPartition(DirectoryInfo directoryInfo, DataFile[] dataFiles, DataCrcFile[] dataCrcFiles)
      {
         string[] nameSplit = directoryInfo.Name.Split('=');
         string key = nameSplit[0];
         string value = nameSplit[1];
         string parent = GetParentPath(directoryInfo, DeltaTable.BasePath);

         return new Partition(parent, key, value, dataFiles, dataCrcFiles);
      }

      private static string GetParentName(string fullPath)
      {
         // TODO linux version
         string[] fullPathArray = fullPath.Split('\\');
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

         return currentFolderType == FolderType.Root && result.dataFileList.Length > 1 && noUnderScoreFolderExist
            ? throw new DeltaException("Root folder can't contain data folders if there are already data files.")
            : result;
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
               (long partIndex, Guid guid, CompressionType compressionType) fileInfo = GetFileInfo(file.Name, file.Extension);

               switch(file.Extension)
               {
                  case Constants.CrcExtension:
                     var crcFile = new DataCrcFile(fileInfo.partIndex, fileInfo.guid.ToString(), file.Length, file.Name);
                     crcFileList[dataFileCounter] = crcFile;
                     dataFileCounter++;
                     break;
                  case Constants.ParquetExtension:
                     var dataFile = new DataFile(fileInfo.partIndex, fileInfo.guid.ToString(), fileInfo.compressionType, file.Length, file.Name);
                     dataFileList[crcFilecounter] = dataFile;
                     crcFilecounter++;
                     break;
                  default:
                     if(_deltaOptions.StrictRootFolderParsing) // TODO refactor in common method with multiple calls
                     {
                        throw new DeltaException("Error file extension not recognised as root folder file.");
                     }
                     else
                     {
                        IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
                        ignoredFileList.Add(ignoredFile);
                     }
                     break;
               }
            }
         }

         return (dataFileList, crcFileList, ignoredFileList);
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

      private Table.DeltaLog ProcessDeltaLog(DirectoryInfo directoryInfo)
      {
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
                     ProcessCrcFile(file, logCrcFiles, ref logCrcFileCounter);
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
         var logFolder = new Table.DeltaLog(logCrcFiles, logFiles, checkPointFiles, lastCheckPointFile, ignoredFileList);

         return logFolder;
      }

      private void ProcessLogFile(FileInfo file, LogFile[] logFiles, ref uint logFileCounter, List<IgnoredFile> ignoredFileList)
      {
         string fileNameWithoutExtension = file.Name[..^Constants.JsonExtension.Length];
         bool isNumbers = Constants.onlyNumbersRegex.IsMatch(fileNameWithoutExtension);
         if(isNumbers)
         {
            var logFile = new LogFile(long.Parse(fileNameWithoutExtension), file.Name);
            logFiles[logFileCounter] = logFile;
            logFileCounter++;
         }
         else
         {
            if(_deltaOptions.StrictRootFolderParsing)
            {
               throw new DeltaException("File name is not only numbers.");
            }
            else
            {
               var ignoredFile = new IgnoredFile(file.FullName);
               ignoredFileList.Add(ignoredFile);
            }
         }
      }

      private static void ProcessCrcFile(FileInfo file, LogCrcFile[] logCrcFiles, ref uint logCrcFileCounter)
      {
         // TODO is strict parsing used here?
         // TODO are ignored files considewred?
         var logCrcFile = new LogCrcFile(file.Name);
         logCrcFiles[logCrcFileCounter] = logCrcFile;
         logCrcFileCounter++;
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
            if(_deltaOptions.StrictRootFolderParsing)
            {
               throw new DeltaException("File extension not recognised as root folder file or name is not only numbers.");
            }
            else
            {
               IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
               ignoredFileList.Add(ignoredFile);
            }
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
         if(_deltaOptions.StrictRootFolderParsing)
         {
            throw new DeltaException("Error file extension not recognised as root folder file.");
         }

         var ignoredFile = new IgnoredFile(file.FullName);
         ignoredFileList.Add(ignoredFile);
      }

      private (long partIndex, Guid guid, CompressionType compressionType) GetFileInfo(string name, string fileExtension)
      {
         name = name.Substring(0, name.Length - fileExtension.Length);
         string[] namePointParts = name.Split('.');
         int infoIndex = string.IsNullOrEmpty(namePointParts[0]) ? 1 : 0;
         string[] nameMinusParts = namePointParts[infoIndex].Split('-');

         if(nameMinusParts[0] == Constants.PartText)
         {
            return namePointParts.Length switch
            {
               // TODO still fails, test with multiple test DATA folders
               1 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), CompressionType.Uncompressed),
               2 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1])),
               3 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
               4 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
               5 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[3])),
               _ => throw new DeltaException($"This parquet part file '{name}' has more dots than expected."),
            };
         }
         else
         {
            throw new DeltaException($"This parquet file '{name}' must start with `part`.");
         }
      }

      private static Guid ExtractGuid(string[] nameParts)
      {
         string guidString = $"{nameParts[2]}-{nameParts[3]}-{nameParts[4]}-{nameParts[5]}-{nameParts[6]}";
         return Guid.Parse(guidString);
         // TODO specific exception ?
      }

      private static CompressionType ExtractCompression(string compressionText) => compressionText switch
      {
         Constants.SnappyText => CompressionType.Snappy,
         _ => throw new DeltaException($"compression not yet supported in this nuget library: '{compressionText}'."),
      };
   }
}
