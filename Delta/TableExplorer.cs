using Delta.Table;
using System.IO;
using System.Xml.Linq;

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
         FileInfo[] files = null;
         DirectoryInfo[] subDirs = null;

         // First, process all the files directly under this folder
         try
         {
            files = root.GetFiles("*.*");
         }
         // This is thrown if even one of the files requires permissions greater
         // than the application provides.
         catch(UnauthorizedAccessException e)
         {
            // This code just writes out the message and continues to recurse.
            // You may decide to do something different here. For example, you
            // can try to elevate your privileges and access the file again.
            ///log.Add(e.Message);
         }

         catch(System.IO.DirectoryNotFoundException e)
         {
            Console.WriteLine(e.Message);
         }

         if(files != null)
         {
            DataFile[] dataFileList = new DataFile[files.Length];
            for(int counter = 0; counter < files.Length; counter++)
            {
               FileInfo file = files[counter];
               FileType fileType = FileType.Unknown;

               if (folderLevel == 1)
               {
                  switch (file.Extension.ToLower()) {
                     case ".crc":
                        fileType = FileType.Crc;
                        break;
                     case ".parquet":
                        fileType = FileType.Parquet;
                        break;
                     default:
                        break;
                  }
                  (long partIndex, Guid guid, bool isCheckpoint, CompressionType compressionType) fileInfo = 
                     GetFileInfo(file.Name, fileType);
                  DataFile dataFile = 
                     new DataFile(
                        fileInfo.partIndex, 
                        fileInfo.guid.ToString(), 
                        fileType, 
                        fileInfo.isCheckpoint, 
                        fileInfo.compressionType);
                  dataFileList[counter] = dataFile;
               }
            }
            _tableFolder.DataFileList = dataFileList.ToArray();

      // Now find all the subdirectories under this directory.
      subDirs = root.GetDirectories();

            foreach(System.IO.DirectoryInfo dirInfo in subDirs)
            {
               // Resursive call for each subdirectory.
               WalkDirectoryTree(dirInfo);
            }
         }
      }

      private (long partIndex, Guid guid, bool isCheckpoint, CompressionType compressionType) 
         GetFileInfo(string name, FileType fileType)
      {
         name = name.Substring(1, name.Length - fileType.ToString().Length - 2);
         string[] namePointParts = name.Split('.');
         string[] nameMinusParts = namePointParts[0].Split('-');
         if(nameMinusParts[0] == "part")
         {
            switch(namePointParts.Length)
            {
               case 1:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), false, CompressionType.Uncompressed);
               case 2:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), false, ExtractCompression(namePointParts[1]));
               case 3:
                  return (long.Parse(nameMinusParts[1]), ExtractGuid(nameMinusParts), false, ExtractCompression(namePointParts[1]));
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
