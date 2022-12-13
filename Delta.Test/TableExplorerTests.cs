using Delta.Table;

namespace Delta.Test
{
    public class TableExplorerTests
    {
      private readonly TableExplorer sut = new TableExplorer();

      [Fact]
      public void Read_Delta_Folder_Structure()
      {
         // Arrange
         string basePath = "Data/delta-0.8.0/";

         // Act
         TableFolder result = sut.ReadDeltaFolderStructure(basePath);

         // Assert
         result.Should().NotBeNull();
      }
   }
}