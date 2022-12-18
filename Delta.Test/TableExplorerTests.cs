using Delta.Common;
using Delta.Table;

namespace Delta.Test
{
    public class TableExplorerTests
    {
      [Fact]
      public void Read_Delta_Folder_Structure()
      {
         // Arrange
         string basePath = "Data\\delta-0.8.0-partitioned\\";
         TableExplorer sut = new TableExplorer(basePath, new DeltaOptions());

         // Act
         sut.ReadDeltaFolderStructure();

         // Assert
         //sut.TableFolder.Should().NotBeNull();
      }
   }
}