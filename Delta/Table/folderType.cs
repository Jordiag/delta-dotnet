using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Delta.Table
{
   /// <summary>
   /// ChangeData: https://docs.databricks.com/delta/delta-change-data-feed.html
   ///   Databricks records change data for UPDATE, DELETE, and MERGE operations in the _change_data folder under the table 
   ///   directory. Some operations, such as insert-only operations and full partition deletes, do not generate data 
   ///   in the _change_data directory because Databricks can efficiently compute the change data feed directly from 
   ///   the transaction log.

   ///   The files in the _change_data folder follow the retention policy of the table. Therefore, 
   ///   if you run the VACUUM command, change data feed data is also deleted.
   ///   
   /// Bloom Filter: https://learn.microsoft.com/en-us/azure/databricks/optimizations/bloom-filters
   ///   A Bloom filter index is an uncompressed Parquet file that contains a single row. Indexes are stored in the _delta_index 
   ///   subdirectory relative to the data file and use the same name as the data file with the suffix index.v1.parquet. 
   ///   For example, the index for data file dbfs:/ db1 / data.0001.parquet.snappy would be named 
   ///   dbfs:/ db1 / _delta_index / data.0001.parquet.snappy.index.v1.parquet.
   /// </summary>
   public enum FolderType
   {
      Root,
      DeltaLog,
      DeltaIndex,
      ChangeData, 
      Partition,
      Unknown
   }
}

