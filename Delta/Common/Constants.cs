using System.Text.RegularExpressions;

namespace Delta.Common
{
   internal static class Constants
   {
      internal const string ParquetExtension = ".parquet";
      internal const string CrcExtension = ".crc";
      internal const string JsonExtension = ".json";
      internal const string CheckPointExtension = ".checkpoint";
      internal const string DeltaLogFolder = "_delta_log";
      internal const string DeltaIndexFolder = "_delta_index";
      internal const string ChangeDataFolder = "_change_data";
      internal const string SnappyCompression = "snappy";
      internal const string PartText = "part";
      internal const string LastCheckPointName = "_last_checkpoint";
      internal static readonly Regex onlyNumbersRegex = new(@"^[0-9]+$");
   }
}
