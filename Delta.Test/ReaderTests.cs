using Delta.Common;
using Delta.Storage;
using Parquet.Data;

namespace Delta.Test
{
    public class ReaderTests
    {
        [Fact]
        public async Task Read_Delta_V0_2_0_Checkpoint_Folder()
        {
            // Arrange
            string basePath = "./Data/delta-0.2.0/_delta_log/00000000000000000003.checkpoint.parquet";

            // Act
            Stream? stream = null;
            FileSystem.GetFileStream(basePath, ref stream);
            SortedList<int, DeltaLog.Actions.IAction> result = await ParquetClient.ReadCheckPointAsync(stream, new DeltaOptions(), basePath);

            // Assert
            result.Count.Should().Be(10);
        }

        [Fact]
        public async Task Read_Full_Delta_Log()
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

