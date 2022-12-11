using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaLake.Research.App.Example2.NewFolder
{
    public class Commitinfo
    {
        public long timestamp { get; set; }
        public string userId { get; set; }
        public string userName { get; set; }
        public string operation { get; set; }
        public Operationparameters operationParameters { get; set; }
        public Job job { get; set; }
        public Notebook notebook { get; set; }
        public string clusterId { get; set; }
        public int readVersion { get; set; }
        public string isolationLevel { get; set; }
        public bool isBlindAppend { get; set; }
        public Operationmetrics operationMetrics { get; set; }
        public string engineInfo { get; set; }
        public string txnId { get; set; }
    }

}
