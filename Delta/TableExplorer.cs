using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Delta.Common;
using Delta.Table;

namespace Delta
{
   public class TableExplorer
   {
      private FolderType currentFolderType = FolderType.Root;
      private readonly DeltaOptions _deltaOptions;

      private PartitionFolder partitionFolderStructure;

      private PartitionFolder previousParentPartitionFolder;
      private PartitionFolder currentParentPartitionFolder;

      private readonly TableFolder tableFolder;

      private readonly Regex onlyNumbersRegex = new Regex(@"^[0-9]+$");

      public TableExplorer(string basePath, DeltaOptions deltaOptions)
      {
         _deltaOptions = deltaOptions;
         tableFolder = new TableFolder(basePath);
      }

      public void ReadDeltaFolderStructure()
      {
         WalkDeltaTree(new DirectoryInfo(tableFolder.BasePath), new List<PartitionFolder>());
      }

      private void WalkDeltaTree(DirectoryInfo directoryInfo, List<PartitionFolder> partitionFolderList)
      {
         DirectoryInfo[] subDirs = Array.Empty<DirectoryInfo>();

         switch(currentFolderType)
         {
            case FolderType.DeltaLog:
               LogFolder deltaLogResult = ProcessDeltaLogFolder(directoryInfo);
               tableFolder.LoadDeltaLog(deltaLogResult);
               break;
            case FolderType.Root:
               (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) rootResult = ProcessRootFolder(directoryInfo, ref subDirs);
               tableFolder.LoadRootDataTable(rootResult.dataFileList, rootResult.crcFileList, rootResult.IgnoredFileList);
               break;
            case FolderType.Partition:
               currentParentPartitionFolder = ProcessPartitionFolder(directoryInfo, ref subDirs);
               break;
            case FolderType.Unknown:
               if(!_deltaOptions.StrictTableParsing)
               {
                  var ignoredFolder = new IgnoredFolder(directoryInfo.FullName);
                  tableFolder.AddIgnoredFolder(ignoredFolder);
               }
               throw new DeltaException($"Unknown type of folder: {directoryInfo.FullName}.");
            default:
               break; 
         }

         foreach(DirectoryInfo dirInfo in subDirs)
         {
            currentFolderType = GetCurrentFolderType(dirInfo);
            WalkDeltaTree(dirInfo, partitionFolderList);
         }
      }

      private PartitionFolder? ProcessPartitionFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs)
      {
         PartitionFolder? partitionFolder = null;
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
            string parent = GetParent(directoryInfo, tableFolder.BasePath);
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            partitionFolder = new PartitionFolder(parent, key, value);
            AddToHeirachy(directoryInfo, partitionFolder);
         }
         else if(files.Length > 0 && subDirs.Length == 0)
         {
            (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> ignoredFileList) dataFolderResult = ProcessDataFolderFiles(directoryInfo);
            string[] nameSplit = directoryInfo.Name.Split('=');
            string key = nameSplit[0];
            string value = nameSplit[1];
            string parent = $"{currentParentPartitionFolder.Key}={currentParentPartitionFolder.Value}";
            currentParentPartitionFolder =
               new PartitionFolder(
                  $"{currentParentPartitionFolder}\\{parent}",
                  key,
                  value,
                  dataFolderResult.dataFileList,
                  dataFolderResult.crcFileList);
         }
         return partitionFolder;
      }

      IEnumerable<PartitionFolder> GetChildren(PartitionFolder x)
      {
         foreach(PartitionFolder rChild in x.FolderList.SelectMany(child => GetChildren(child)))
         {
            yield return rChild;
         }
      }

      private string GetParentName(string fullPath)
      {
         // TODO linux version
         string[] fullPathArray = fullPath.Split('\\');
         string parent = fullPathArray[fullPathArray.Length - 2];

         return parent;
      }

      private void AddToHeirachy(DirectoryInfo directoryInfo, PartitionFolder partitionFolder)
      {
         if(partitionFolderStructure == null)
         {
            partitionFolderStructure = new PartitionFolder(tableFolder.BasePath);
            partitionFolderStructure.FolderList.Add(partitionFolder);
         }
         else
         {
            string parentName = GetParentName(directoryInfo.FullName);
            PartitionFolder? parentFolder = partitionFolderStructure.FolderList
               .Where(folder => $"{folder.Key}={folder.Value}" == parentName).SingleOrDefault();
               //.SelectMany(folder => GetChildren(folder)).SingleOrDefault();

            if (parentFolder == null)
            {
               partitionFolderStructure.FolderList.Add(partitionFolder);
            }
            else
            {
               parentFolder?.FolderList.Add(partitionFolder);
            }

         }
      }

      private string GetParent(DirectoryInfo directoryInfo, string basePath)
      {
         int d = directoryInfo.FullName.IndexOf(basePath);
         string path = directoryInfo.FullName.Substring(d);
         path = path.Substring(0, path.Length - directoryInfo.Name.Length);

         return path;

      }

      private (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) ProcessRootFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[]  subDirs)
      {
         (DataFile[] dataFileList, DataCrcFile[] crcFileList, List<IgnoredFile> IgnoredFileList) result = ProcessDataFolderFiles(directoryInfo);
         subDirs = directoryInfo.GetDirectories();
         bool noUnderScoreFolderExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

         if(currentFolderType == FolderType.Root && result.dataFileList.Length > 1 && noUnderScoreFolderExist)
         {
            throw new DeltaException("Root folder can't contain data folders if there are already data files.");
         }

         return result;
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

      private (DataFile[] dataFileList, DataCrcFile[] crcFileList,List<IgnoredFile> ignoredFileList) ProcessDataFolderFiles(DirectoryInfo directoryInfo) 
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
                     if(_deltaOptions.StrictRootFolderParsing)
                     {
                        throw new DeltaException("Error file extension not recognised as root folder file.");
                     }
                     else
                     {
                        IgnoredFile ignoredFile  = new IgnoredFile(file.FullName);
                        ignoredFileList.Add(ignoredFile);
                     }
                  break;
               }
            }
         }

         return (dataFileList, crcFileList, ignoredFileList);
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

      private LogFolder ProcessDeltaLogFolder(DirectoryInfo directoryInfo)
      {
         FileInfo[] files = GetFiles(directoryInfo);
         var logCrcFileList = new LogCrcFile[files.Count(file => file.Extension == Constants.CrcExtension)];
         var logFileList = new LogFile[files.Count(file => file.Extension == Constants.JsonExtension)];
         var checkPointFileList = new CheckPointFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
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
                           ignoredFileList.Add(ignoredFile);
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
                           ignoredFileList.Add(ignoredFile);
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
                        ignoredFileList.Add(ignoredFile);
                     }
                     break;
               }
            }
         }
         var logFolder = new LogFolder(logCrcFileList, logFileList, checkPointFileList, lastCheckPointFile, ignoredFileList);

         return logFolder;
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
               2 or 3 => (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2])),
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

