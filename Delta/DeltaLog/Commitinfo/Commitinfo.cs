namespace Delta.DeltaLog.Commitinfo
{
    public class Commitinfo
    {
        public long Timestamp { get; set; }
        public string UserId { get; set; }
        public string UserName { get; set; }
        public string Operation { get; set; }
        public Operationparameters OperationParameters { get; set; }
        public Job Job { get; set; }
        public Notebook Notebook { get; set; }
        public string ClusterId { get; set; }
        public string IsolationLevel { get; set; }
        public bool IsBlindAppend { get; set; }
        public Operationmetrics OperationMetrics { get; set; }
        public string EngineInfo { get; set; }
        public string TxnId { get; set; }
    }
}