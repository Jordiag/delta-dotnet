namespace Delta.Table
{
   internal class CheckPointFile
   {
      internal long Number { get; }
      internal string Name { get; }

      public CheckPointFile(long number, string name)
      {
         Number = number;
         Name = name;
      }
   }
}