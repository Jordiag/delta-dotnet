using Delta.Common;
using Delta.Test.Tools;
using Parquet.Data;

namespace Delta.Test
{
    public class ReaderTests
    {
        [Fact]
        public async Task Read_Delta_V0_2_0_Checkpoint_FolderAsync()
        {
            // Arrange
            string basePath = ".\\Data\\delta-0.2.0\\_delta_log\\00000000000000000003.checkpoint.parquet";

            // Act
            (List<string[]> idsList, Schema schema, DataColumn[] data) result = await ParquetUtils.ReadAsync(basePath);

            // Assert
            result.data.Should().NotBeNull();
        }

        [Fact]
        public async Task Read_Delta_V0_2_0_Simple_FolderAsync()
        {
            // Arrange
            string basePath = ".\\Data\\delta-0.2.0-simple\\";
            var sut = new Reader(basePath, new DeltaOptions());

            // Act
            DataColumn[] data = sut.GetTable();

            // Assert
            data.Should().HaveCountGreaterThan(1);
        }
    }
}
