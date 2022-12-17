namespace Delta.Table
{
   internal class LogFile
   {
      internal long Number { get; }
      internal string Name { get; }

      public LogFile(long number, string name)
      {
         Number = number;
         Name = name;
      }
   }
}