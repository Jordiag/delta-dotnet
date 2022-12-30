using Delta.Common;
using Delta.DeltaLog;
using Delta.Storage;
using Parquet.Data;

namespace Delta.Test
{
    public class ReaderTests
    {
        [Fact]
        public async Task Read_Delta_V0_2_0_Checkpoint_FolderAsync()
        {
            // Arrange
            string basePath = "./Data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";

            // Act
            Stream? stream = null;
            FileSystem.GetFileStream(basePath, ref stream);
            CheckPoint result = await ParquetClient.ReadCheckPointAsync(stream);

            // Assert
            result.Adds.Should().BeNull();
        }

        [Fact]
        public async Task Read_Delta_FolderAsync()
        {
            // Arrange
            string basePath = "./Data/delta-0.2.0/";

            var sut = new Reader(basePath, new DeltaOptions());

            // Act
            DataColumn[] data = await sut.GetTableAsync();

            // Assert
            data.Should().HaveCountGreaterThan(-1);
        }
    }
}

