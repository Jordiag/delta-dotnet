using System.Text.RegularExpressions;
using Delta.Common;
using Delta.Table;

namespace Delta
{
   public class TableExplorer
   {
      private FolderType currentFolderType = FolderType.Root;
      private readonly DeltaOptions _deltaOptions;
      private readonly List<IgnoredFile> _ignoredFileList;
      private readonly List<IgnoredFolder> _ignoredFolderList;
      private readonly string _basePath;

      private DataFile[] dataFileList;
      private DataCrcFile[] crcFileList;
      private readonly List<PartitionFolder> partitionFolderList = new();
      private PartitionFolder currentPartitionFolder;
      private string parentFolder;

      private LogCrcFile[] logCrcFileList;
      private LogFile[] logFileList;
      private CheckPointFile[] checkPointFileList;
      private LastCheckPointFile lastCheckPointFile;

      private readonly Regex onlyNumbersRegex = new Regex(@"^[0-9]+$");

      public TableExplorer(string basePath, DeltaOptions deltaOptions)
      {
         _deltaOptions = deltaOptions;
         _ignoredFolderList = new List<IgnoredFolder>();
         _ignoredFileList = new List<IgnoredFile>();
         _basePath = basePath;
      }

      public void ReadDeltaFolderStructure()
      {
         WalkDeltaTree(new DirectoryInfo(_basePath));
         LogFolder logFolder = new LogFolder(logCrcFileList, logFileList, checkPointFileList, lastCheckPointFile);
         TableFolder tableFolder = new TableFolder(_basePath, logFolder, dataFileList, crcFileList, partitionFolderList);
      }

      private void WalkDeltaTree(DirectoryInfo DirectoryInfo)
      {
         DirectoryInfo[] subDirs = Array.Empty<DirectoryInfo>();

         switch(currentFolderType)
         {
            case FolderType.DeltaLog:
               ProcessDeltaLogFolder(DirectoryInfo);
               break;
            case FolderType.Root:
               ProcessRootFolder(DirectoryInfo, ref subDirs);
               break;
            case FolderType.Partition:
               ProcessPartitionFolder(DirectoryInfo, ref subDirs);
               break;
            case FolderType.Unknown:
               if(!_deltaOptions.StrictTableParsing)
               {
                  var ignoredFolder = new IgnoredFolder(DirectoryInfo.FullName);
                  _ignoredFolderList.Add(ignoredFolder);
               }
               throw new DeltaException($"Unknown type of folder: {DirectoryInfo.FullName}.");
             default:
               break;
         }

         parentFolder = DirectoryInfo.FullName;

         foreach(DirectoryInfo dirInfo in subDirs)
         {
            currentFolderType = GetCurrentFolderType(dirInfo);
            WalkDeltaTree(dirInfo);
         }
      }

      private void ProcessPartitionFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs)
      {
         FileInfo[] files = GetFiles(directoryInfo);
         subDirs = directoryInfo.GetDirectories();

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
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            var partitionFolder = new PartitionFolder(key, value);
            if(partitionFolderList.Contains(currentPartitionFolder))
            {
               //currentPartitionFolder.Folder = partitionFolder;
            }
            else
            {
               partitionFolderList.Add(partitionFolder);
               currentPartitionFolder = partitionFolder;
            }

         }
         else if(files.Length > 0 && subDirs.Length == 0)
         {
            ProcessPartitionDataFolder(directoryInfo);
         }
      }

      private void ProcessPartitionDataFolder(DirectoryInfo directoryInfo) => throw new NotImplementedException();

      private void ProcessRootFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[]  subDirs)
      {
         ProcessRootFolderFiles(directoryInfo);
         subDirs = directoryInfo.GetDirectories();
         bool noUnderScoreFolderExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

         if(currentFolderType == FolderType.Root && dataFileList.Length > 1 && noUnderScoreFolderExist)
         {
            throw new DeltaException("Root folder can't contain data folders if there are already data files.");
         }
      }

      private static FolderType GetCurrentFolderType(DirectoryInfo dirInfo)
      {
         return dirInfo.Name switch
         {
            Constants.DeltaLogFolder => FolderType.DeltaLog,
            Constants.DeltaIndexFolder => FolderType.DeltaIndex,
            Constants.ChangeDataFolder => FolderType.ChangeData,
            _ => IsPartitionFolder(dirInfo.Name) ? FolderType.Partition : FolderType.Unknown,
         };
      }

      private static bool IsPartitionFolder(string name)
      {
         return name.Contains('=') && name.Split('=').Length == 2;
      }

      private void ProcessRootFolderFiles(DirectoryInfo directoryInfo) 
      {
         FileInfo[] files = GetFiles(directoryInfo);

         if(files != null)
         {
            dataFileList = new DataFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            crcFileList = new DataCrcFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
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
                     if(_deltaOptions.StrictRootFolderParsing)
                     {
                        throw new DeltaException("Error file extension not recognised as root folder file.");
                     }
                     else
                     {
                        IgnoredFile ignoredFile  = new IgnoredFile(file.FullName);
                        _ignoredFileList.Add(ignoredFile);
                     }
                  break;
               }
            }
         }
      }

      private FileInfo[] GetFiles(DirectoryInfo directoryInfo)
      {
         FileInfo[] files = Array.Empty<FileInfo>();
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

      private void ProcessDeltaLogFolder(DirectoryInfo directoryInfo)
      {
         FileInfo[] files = GetFiles(directoryInfo);

         if(files != null)
         {
            logCrcFileList = new LogCrcFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            logFileList = new LogFile[files.Count(file => file.Extension == Constants.JsonExtension)];
            checkPointFileList = new CheckPointFile[files.Count(file => file.Extension == Constants.ParquetExtension)];

            uint logCrcFileCounter = 0;
            uint logFileCounter = 0;
            uint checkPointFileCounter = 0;

            for(int counter = 0; counter < files.Length; counter++)
            {
               FileInfo file = files[counter];
               switch(file.Extension)
               {
                  case Constants.JsonExtension:
                     string fileNameWithoutExtension = file.Name.Substring(0, file.Name.Length - Constants.JsonExtension.Length);
                     bool isNumbers = onlyNumbersRegex.IsMatch(fileNameWithoutExtension);
                     if(isNumbers)
                     {
                        var logFile = new LogFile(long.Parse(fileNameWithoutExtension), file.Name);
                        logFileList[logFileCounter] = logFile;
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
                           IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
                           _ignoredFileList.Add(ignoredFile);
                        }
                     }
                     break;
                  case Constants.CrcExtension:
                     var logCrcFile = new LogCrcFile(file.Name);
                     logCrcFileList[logCrcFileCounter] = logCrcFile;
                     logCrcFileCounter++;
                     break;
                  case Constants.ParquetExtension:
                     fileNameWithoutExtension = file.Name.Substring(0, file.Name.Length - Constants.ParquetExtension.Length - Constants.CheckPointExtension.Length);
                     
                     isNumbers = onlyNumbersRegex.IsMatch(fileNameWithoutExtension);
                     if(file.Name.EndsWith($"{Constants.CheckPointExtension}{Constants.ParquetExtension}") && isNumbers)
                     {
                        var checkPoint = new CheckPointFile(long.Parse(fileNameWithoutExtension), file.Name);
                        checkPointFileList[checkPointFileCounter] = checkPoint;
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
                           _ignoredFileList.Add(ignoredFile);
                        }
                     }
                     break;
                  default:
                     if (file.Name == Constants.LastCheckPointName)
                     {
                        int LastCheckPointFileNumber = files.Count(file => file.Name == Constants.LastCheckPointName);
                        if(LastCheckPointFileNumber > 1)
                        {
                           throw new DeltaException($"Only one '{Constants.LastCheckPointName}' is allowed.");
                        }
                        lastCheckPointFile = new LastCheckPointFile(file.Name);
                        break;
                     }
                     if(_deltaOptions.StrictRootFolderParsing)
                     {
                        throw new DeltaException("Error file extension not recognised as root folder file.");
                     }
                     else
                     {
                        IgnoredFile ignoredFile = new IgnoredFile(file.FullName);
                        _ignoredFileList.Add(ignoredFile);
                     }
                     break;
               }
            }
         }
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
               1 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), CompressionType.Uncompressed),
               2 or 3 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1])),
               4 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
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
      }

      private static CompressionType ExtractCompression(string compressionText)
      {
         switch(compressionText)
         {
            case Constants.SnappyCompression:
               return CompressionType.Snappy;
            default:
               throw new DeltaException($"compression not yet supported in this nuget library: '{compressionText}'.");
         }
      }
   }
}

