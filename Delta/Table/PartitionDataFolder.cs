namespace Delta.Table
{
   internal class PartitionDataFolder
   {
      internal string Key { get; }
      internal string Value { get; }

      internal PartitionDataFolder(string key, string value)
      {
         Key = key;
         Value = value; 
      }
   }
}
