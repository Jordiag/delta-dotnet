namespace DeltaLake.BaseFolder
{
    public class DeltaFolder
    {
        private readonly string _basePath;
        public DeltaLog[] DeltaLog { get; set; }
        public ChangeData[] ChangeData { get; set; }
        public Parquet[] Parquet { get; set; }

        public DeltaFolder(string basePath) 
        {
            _basePath = basePath;
            Parquet = GetParquetFiles();
            DeltaLog = GetDeltaLog();
            ChangeData = GetChangeData();
        }

        public void GetLatestSnapshot()
        {

        }

        private ChangeData[] GetChangeData()
        {
            throw new NotImplementedException();
        }

        private DeltaLog[] GetDeltaLog()
        {
            throw new NotImplementedException();
        }

        private Parquet[] GetParquetFiles()
        {
            throw new NotImplementedException();
        }

    }
}
