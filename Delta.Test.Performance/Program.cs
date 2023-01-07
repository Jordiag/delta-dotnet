using BenchmarkDotNet.Running;
using Delta.Test.Performance.Explorer;

namespace Delta.Test.Performance
{
    internal class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<ExplorerPerformance>();
        }
    }
}