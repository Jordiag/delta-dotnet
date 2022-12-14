using Delta.Table;

namespace Delta
{
   public class TableExplorer
   {
      private int folderLevel = 0;
      private TableFolder _tableFolder = null;

      public TableFolder ReadDeltaFolderStructure(string basePath)
      {
         _tableFolder = new TableFolder(basePath);
         WalkDirectoryTree(new DirectoryInfo(_tableFolder.RootPath));

         return _tableFolder;
      }

      public void WalkDirectoryTree(DirectoryInfo root)
      {
         folderLevel++;
         DirectoryInfo[] subDirs = null;

         if(root.Name == Constants.DeltaLogFolder)
         {
            if (folderLevel != 2)
            {
               throw new DeltaException("Error trying to get files regarding unauthorised access.");
            }
            ProcessDeltaLogFolder(root);
         }

         ProcessFolderFiles(root, ref _tableFolder, folderLevel);

         // Now find all the subdirectories under this directory.
         subDirs = root.GetDirectories();

         bool noUnderScoreFolderExist = !subDirs.All(subdir => subdir.Name.StartsWith("_"));

         if (folderLevel == 1 && _tableFolder.DataFileList.Length > 1 && noUnderScoreFolderExist)
         {
            throw new DeltaException("Root folder can't contain data folders if there are already data files.");
         }

         foreach(DirectoryInfo dirInfo in subDirs)
         {
            // Resursive call for each subdirectory.
            WalkDirectoryTree(dirInfo);
         }

         // TODO me quede en procesar el deltalog folder y que hacer ahora con change y delta_index folder
      }

      private void ProcessFolderFiles(DirectoryInfo root, ref TableFolder tableFolder, int folderLevel) {
         FileInfo[] files = null;
         try
         {
            files = root.GetFiles("*.*");
         }
         catch(UnauthorizedAccessException e)
         {
            throw new DeltaException("Error trying to get files regarding unauthorised access.", e);
         }

         catch(System.IO.DirectoryNotFoundException e)
         {
            throw new DeltaException("Error trying to get files regarding directory not found.", e);
         }

         if(files != null)
         {
            DataFile[] dataFileList = new DataFile[files.Count(file => file.Extension == Constants.CrcExtension)];
            CrcFile[] crcFileList = new CrcFile[files.Count(file => file.Extension == Constants.ParquetExtension)];
            uint dataFileCounter = 0;
            uint crcFilecounter = 0;
            for(int counter = 0; counter < files.Length; counter++)
            {
               FileInfo file = files[counter];

               if(folderLevel == 1) // we can have DataFiles and CrcFiles Only, if they are there, no other data folders must exist
               {
                  (long partIndex, Guid guid, CompressionType compressionType) fileInfo = GetFileInfo(file.Name, file.Extension);

                  switch(file.Extension.ToLower())
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
                        throw new DeltaException("Error file extension not recognised as root folder file.");
                  }

               }
            }
            tableFolder.DataFileList = dataFileList.ToArray();
            tableFolder.CrcFileList = crcFileList.ToArray();
         }
      }

      private void ProcessDeltaLogFolder(DirectoryInfo root)
      {
      }

      private (long partIndex, Guid guid, CompressionType compressionType) 
         GetFileInfo(string name, string fileExtension)
      {
         name = name.Substring(0, name.Length - fileExtension.Length);
         string[] namePointParts = name.Split('.');
         int infoIndex = string.IsNullOrEmpty(namePointParts[0]) ? 1 : 0;
         string[] nameMinusParts = namePointParts[infoIndex].Split('-');

         if(nameMinusParts[0] == "part")
         {
            switch(namePointParts.Length)
            {
               case 1:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), CompressionType.Uncompressed);
               case 2:
               case 3:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[1]));
               case 4:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), ExtractCompression(namePointParts[2]));
               default:
                  throw new DeltaException($"This parquet part file '{name}' has more dots than expected.");
            }

         }
         else
         {
            throw new DeltaException($"This parquet file '{name}' must start with `part`.");
         }

      }

      private Guid ExtractGuid(string[] nameParts)
      {
         string guidString = $"{nameParts[2]}-{nameParts[3]}-{nameParts[4]}-{nameParts[5]}-{nameParts[6]}";
         return Guid.Parse(guidString);
      }

      private CompressionType ExtractCompression(string compressionText)
      {
         switch(compressionText)
         {
            case "snappy":
               return CompressionType.Snappy;
            default:
               throw new DeltaException($"compression not yet supported in this nuget library: '{compressionText}'.");
         }
      }
   }
}
