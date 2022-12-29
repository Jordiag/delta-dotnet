using Delta.Common;
using Delta.DeltaStructure;
using Delta.Storage;

namespace Delta.Test
{
    public class ExplorerTests
    {
        [Theory]
        [InlineData("./Data/delta-0.2.0-simple/")]
        [InlineData("./Data/delta-0.2.0/")]
        [InlineData("./Data/delta-0.8.0/")]
        [InlineData("./Data/delta-0.8.0-date/")]
        [InlineData("./Data/delta-0.8.0-null-partition/")]
        [InlineData("./Data/delta-0.8.0-numeric-partition/")]
        [InlineData("./Data/delta-0.8.0-partitioned/")]
        [InlineData("./Data/delta-0.8.0-special-partition/")]
        [InlineData("./Data/delta-0.8-empty/")]
        [InlineData("./Data/delta-1.2.1-only-struct-stats/")]
        [InlineData("./Data/delta-2.2.0-partitioned-types/")]
        public void Read_Delta_Directory_Structure(string basePath)
        {
            // Arrange
            var sut = new Explorer(basePath, new DeltaOptions(true));

            // Act
            DeltaTable deltaTable = sut.ReadStructure<FileSystemDir>();

            // Assert
            deltaTable.IsEmpty().Should().Be(false);
        }
    }
}
