namespace Delta.Table
{
   public enum CompressionType
   {
      Uncompressed = 0,
      Snappy = 1,
      Gzip = 2,
      Lzo = 3,
      Brotli = 4,  // Added in 2.4
      Lz4 = 5,     // DEPRECATED (Added in 2.4)
      Zstd = 6,    // Added in 2.4
      Lz4Raw = 7 // Added in 2.9
   }
}
