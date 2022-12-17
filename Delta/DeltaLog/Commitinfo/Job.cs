namespace Delta.DeltaLog.Commitinfo
{
    public class Job
    {
        public string JobId { get; set; }
        public string JobName { get; set; }
        public string RunId { get; set; }
        public string JobOwnerId { get; set; }
        public string TriggerType { get; set; }
    }
}