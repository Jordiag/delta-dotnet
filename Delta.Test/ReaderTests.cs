using Delta.Common;
using Delta.DeltaStructure;

namespace Delta.Test
{
    public class ReaderTests
    {
        [Fact]
        public void Read_Delta_V0_2_0_Data_Folder()
        {
            // Arrange
            string basePath = "\\Data\\delta-0.2.0\\";
            var sut = new Reader(basePath, new DeltaOptions(true));

            // Act


            // Assert

        }
    }
}

