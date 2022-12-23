using Delta.Common;

namespace Delta.Test
{
    public class TableExplorerTests
    {
        [Fact]
        public void Read_Delta_Folder_Structure()
        {
            // Arrange
            string basePath = Path.Combine("\\Data\\delta-0.8.0-partitioned\\");
            var sut = new DeltaTableExplorer(basePath, new DeltaOptions());

            // Act
            sut.ReadDeltaFolderStructure();

            // Assert
            sut.DeltaTable.Should().NotBeNull();
        }
    }
}

