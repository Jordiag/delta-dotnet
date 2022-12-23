using Delta.Common;
using Delta.DeltaStructure;

namespace Delta.Test
{
    public class ExplorerTests
    {
        [Fact]
        public void Read_Delta_Directory_Structure()
        {
            // Arrange
            string basePath = "\\Data\\delta-0.8.0-partitioned\\";
            var sut = new Explorer(basePath, new DeltaOptions(true));

            // Act
            DeltaTable deltaTable = sut.ReadStructure();

            // Assert
            deltaTable.Should().NotBeNull();
        }
    }
}

