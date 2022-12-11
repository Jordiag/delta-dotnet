using DeltaLake.BaseFolder;

namespace DeltaLake
{
    public class FolderExplorer
    {
        public void ReadDeltaFoderStructure(string basePath)
        {
            var deltaFolder = new DeltaFolder(basePath);
        }
    }
}
