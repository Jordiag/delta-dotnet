using System.Text.RegularExpressions;

namespace Delta.Common
{
   internal static class Constants
   {
      internal const string ParquetExtension = ".parquet";
      internal const string CrcExtension = ".crc";
      internal const string JsonExtension = ".json";
      internal const string CheckPointExtension = ".checkpoint";
      internal const string DeltaLogName = "_delta_log";
      internal const string DeltaIndexName = "_delta_index";
      internal const string ChangeDataName = "_change_data";
      internal const string SnappyText = "snappy";
      internal const string PartText = "part";
      internal const string LastCheckPointName = "_last_checkpoint";
      internal static readonly Regex onlyNumbersRegex = new(@"^[0-9]+$", RegexOptions.None, TimeSpan.FromMilliseconds(100));
   }
}
