using Parquet.Data;
using Parquet.Data.Rows;

namespace Delta.Common
{
    /// <summary>
    /// Delta table sorted List of rows.
    /// </summary>
    public class ParquetDeltaTable : HashSet<Row>
    {
        /// <summary>
        /// Table Schema.
        /// </summary>
        public Schema Schema { get; }

        /// <summary>
        /// Creates instance of Delta table adding schema.
        /// </summary>
        /// <param name="schema"></param>
        public ParquetDeltaTable(Schema schema)
            => Schema = schema ?? throw new DeltaException("Schema can't be null.");
    }
}
