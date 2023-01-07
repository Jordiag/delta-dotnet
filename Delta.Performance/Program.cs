using BenchmarkDotNet.Running;
using Delta.Performance.Explorer;

namespace Delta.Performance
{
    internal class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<ExplorerPerformance>();
        }
    }
}