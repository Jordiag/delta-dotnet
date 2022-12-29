using Delta.DeltaLog;
using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Delta.Common
{
    /// <summary>
    /// Parquet Client to read and write parquet files.
    /// </summary>
    public class ParquetClient
    {
        /// <summary>
        /// Empty constructor.
        /// </summary>
        protected ParquetClient()
        {
        }

        /// <summary>
        /// Read parquet file from a path.
        /// </summary>
        /// <param name="fileStream">FileStream to read</param>
        /// <returns></returns>
        public static async Task<CheckPoint> ReadCheckPointAsync(Stream fileStream)
        {
            var idsList = new List<string[]>();
            // open parquet file reader
            using(ParquetReader parquetReader = await ParquetReader.CreateAsync(fileStream))
            {
                // get file schema (available straight after opening parquet reader)
                // however, get only data fields as only they contain data values
                DataField[] dataFields = parquetReader.Schema.GetDataFields();
                // enumerate through row groups in this file
                for(int i = 0; i < parquetReader.RowGroupCount; i++)
                {
                    // create row group reader
                    using(ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                    {
                        // read all columns inside each row group (you have an option to read only
                        // required columns if you need to.
                        var columns = new DataColumn[dataFields.Length];
                        for(int c = 0; c < columns.Length; c++)
                        {
                            columns[c] = await groupReader.ReadColumnAsync(dataFields[c]);
                        }

                        // get first column, for instance
                        DataColumn firstColumn = columns[0];

                        // .Data member contains a typed array of column data you can cast to the type of the column
                        Array data = firstColumn.Data;
                        string[] ids = (string[])data;
                        idsList.Add(ids);
                    }
                }
                var checkpoint = new CheckPoint();

                return checkpoint;
            }
        }

        public static async Task<T> ReadDataAsync<T>(Stream fileStream)
        {
            throw new NotImplementedException();
        }
    }
}


