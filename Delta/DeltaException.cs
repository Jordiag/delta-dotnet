namespace Delta
{
   /// <summary>
   /// Parquet format specific
   /// </summary>
   public class DeltaException : Exception
   {
      /// <summary>
      /// Creates an instance
      /// </summary>
      public DeltaException() { }

      /// <summary>
      /// Creates an instance
      /// </summary>
      public DeltaException(string message) : base(message) { }

      /// <summary>
      /// Creates an instance
      /// </summary>
      public DeltaException(string message, Exception inner) : base(message, inner) { }
   }
}
