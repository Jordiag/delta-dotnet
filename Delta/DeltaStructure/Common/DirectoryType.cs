namespace Delta.DeltaStructure.Common
{
    /// <summary>
    /// ChangeData: https://docs.databricks.com/delta/delta-change-data-feed.html
    ///   Databricks records change data for UPDATE, DELETE, and MERGE operations in the _change_data directory under the table 
    ///   directory. Some operations, such as insert-only operations and full partition deletes, do not generate data 
    ///   in the _change_data directory because Databricks can efficiently compute the change data feed directly from 
    ///   the transaction log.
    ///   The files in the _change_data directory follow the retention policy of the table. Therefore, 
    ///   if you run the VACUUM command, change data feed data is also deleted.
    ///   
    /// Bloom Filter: https://learn.microsoft.com/en-us/azure/databricks/optimizations/bloom-filters
    ///   A Bloom filter index is an uncompressed Parquet file that contains a single row. Indexes are stored in the _delta_index 
    ///   subdirectory relative to the data file and use the same name as the data file with the suffix index.v1.parquet. 
    ///   For example, the index for data file dbfs:/ db1 / data.0001.parquet.snappy would be named 
    ///   dbfs:/ db1 / _delta_index / data.0001.parquet.snappy.index.v1.parquet.
    /// </summary>
    public enum DirectoryType
    {
        /// <summary>
        /// The root folder of the Delta Lake Table.
        /// </summary>
        Root,
        /// <summary>
        /// The delta log folder of the Delta Lake Table.
        /// </summary>
        DeltaLog,
        /// <summary>
        /// The delta index folder of the Delta Lake Table located inside root folder.
        /// </summary>
        DeltaIndex,
        /// <summary>
        /// The change data folder of the Delta Lake Table located inside root folder.
        /// </summary>
        ChangeData,
        /// <summary>
        /// The partition folder of the Delta Lake Table located inside root folder.
        /// </summary>
        Partition,
        /// <summary>
        /// Unknown Delta Lake Table folder type.
        /// </summary>
        Unknown
    }
}

