namespace Delta.Table
{
   public class TableLogFile
   {
      long Index { get; set; }
      string Name { get; set; }
      long Bytes { get; set; }
      FileType FileType { get; set; }
   }
}
