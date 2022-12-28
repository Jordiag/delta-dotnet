namespace Delta.DeltaStructure.Common
{
    /// <summary>
    /// Compression type used in parquet format
    /// </summary>
    public enum CompressionType
    {
        /// <summary>
        /// No compression
        /// </summary>
        Uncompressed = 0,
        /// <summary>
        /// Snappy (previously known as Zippy) is a fast data compression and decompression library written in C++ by Google based on ideas from LZ77 and open-sourced in 2011.
        /// </summary>
        Snappy = 1,
        /// <summary>
        /// Gzip is a file format and a software application used for file compression and decompression. 
        /// The program was created by Jean-loup Gailly and Mark Adler as a free software replacement for the compress program used in early Unix systems, 
        /// and intended for use by GNU (from where the "g" of gzip is derived).
        /// </summary>
        Gzip = 2,
        /// <summary>
        /// Lempel–Ziv–Oberhumer (LZO) is a lossless data compression algorithm that is focused on decompression speed.
        /// </summary>
        Lzo = 3,
        /// <summary>
        /// Brotli is a lossless data compression algorithm developed by Google. 
        /// It uses a combination of the general-purpose LZ77 lossless compression algorithm, Huffman coding and 2nd-order context modelling.
        /// </summary>
        Brotli = 4,  // Added in 2.4
        /// <summary>
        /// LZ4 is a lossless data compression algorithm that is focused on compression and decompression speed. 
        /// It belongs to the LZ77 family of byte-oriented compression schemes.
        /// </summary>
        Lz4 = 5,     // DEPRECATED (Added in 2.4)
        /// <summary>
        /// Zstandard, commonly known by the name of its reference implementation zstd, is a lossless data compression algorithm developed by Yann Collet at Facebook. 
        /// Zstd is the reference implementation in C. Version 1 of this implementation was released as open-source software on 31 August 2016.
        /// </summary>
        Zstd = 6,    // Added in 2.4
        /// <summary>
        /// Like LZ4 but raw.
        /// </summary>
        Lz4Raw = 7 // Added in 2.9
    }
}
