using System.Runtime;
using Delta.Common;
using Delta.Table;

namespace Delta
{
   public class TableExplorer
   {
      public TableFolder TableFolder { get; }
      private FolderType currentFolderType = FolderType.Root;
      private readonly DeltaOptions _deltaOptions;
      private readonly List<IgnoredFile> _ignoredFileList;
      private readonly List<IgnoredFolder> _ignoredFolderList;

      public TableExplorer(string basePath, DeltaOptions deltaOptions)
      {
         TableFolder = new TableFolder(basePath);
         _deltaOptions = deltaOptions;
         _ignoredFolderList = new List<IgnoredFolder>();
         _ignoredFileList = new List<IgnoredFile>();
      }

      public void ReadDeltaFolderStructure()
      {
         WalkDeltaTree(new DirectoryInfo(TableFolder.RootPath));
      }

      private void WalkDeltaTree(DirectoryInfo DirectoryInfo)
      {
         DirectoryInfo[] subDirs = Array.Empty<DirectoryInfo>();

         switch(currentFolderType)
         {
            case FolderType.DeltaLog:
               ProcessDeltaLogFolder(DirectoryInfo, ref subDirs);
               break;
            case FolderType.Root:
               ProcessRootFolder(DirectoryInfo, ref subDirs);
               break;
            case FolderType.PartitionData:
               ProcessRootFolder(DirectoryInfo, ref subDirs);
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

         foreach(DirectoryInfo dirInfo in subDirs)
         {
            currentFolderType = GetCurrentFolderType(dirInfo);
            WalkDeltaTree(dirInfo);
         }
      }

      private void ProcessRootFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[]  subDirs)
      {
         ProcessRootFolderFiles(directoryInfo);
         subDirs = directoryInfo.GetDirectories();
         bool noUnderScoreFolderExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

         if(currentFolderType == FolderType.Root && TableFolder.DataFileList.Length > 1 && noUnderScoreFolderExist)
         {
            throw new DeltaException("Root folder can't contain data folders if there are already data files.");
         }
      }

      private static FolderType GetCurrentFolderType(DirectoryInfo dirInfo)
      {
         return dirInfo.Name switch
         {
            Constants.DeltaLogFolder => FolderType.DeltaIndex,
            Constants.DeltaIndexFolder => FolderType.DeltaIndex,
            Constants.ChangeDataFolder => FolderType.ChangeData,
            _ => IsPartitionDataFoder(dirInfo.Name) ? FolderType.PartitionData : FolderType.Unknown,
         };
      }

      private static bool IsPartitionDataFoder(string name)
      {
         return name.Contains('=') && name.Split('=').Length == 2;
      }

      private void ProcessRootFolderFiles(DirectoryInfo directoryInfo) 
      {
         FileInfo[] files = GetFiles(directoryInfo);

         if(files != null)
         {
            var dataFileList = new DataFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            var crcFileList = new CrcFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
            uint dataFileCounter = 0;
            uint crcFilecounter = 0;
            for(int counter = 0; counter < files.Length; counter++)
            {
               FileInfo file = files[counter];
               (long partIndex, Guid guid, CompressionType compressionType) fileInfo = GetFileInfo(file.Name, file.Extension);

               switch(file.Extension)
               {
                  case Constants.CrcExtension:
                     var crcFile = new CrcFile(fileInfo.partIndex, fileInfo.guid.ToString(), file.Length);
                     crcFileList[dataFileCounter] = crcFile;
                     dataFileCounter++;
                     break;
                  case Constants.ParquetExtension:
                     var dataFile = new DataFile(fileInfo.partIndex, fileInfo.guid.ToString(), fileInfo.compressionType, file.Length);
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
            TableFolder.DataFileList = dataFileList.ToArray();
            TableFolder.CrcFileList = crcFileList.ToArray();
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

      private void ProcessDeltaLogFolder(DirectoryInfo directoryInfo, ref DirectoryInfo[] subDirs)
      {
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
