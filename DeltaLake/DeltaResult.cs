namespace DeltaLake
{
    public class DeltaResult<T>
    {
        public int PageNumber { get; set; }

        public int PagesCount { get; set; }

        public T Data { get; set; }
    }
}
