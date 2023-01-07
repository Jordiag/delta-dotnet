using BenchmarkDotNet.Attributes;

namespace Delta.Test.Performance.Explorer
{
    [MemoryDiagnoser]
    internal partial class ExplorerPerformance
    {
        [Benchmark]
        public void SampleTestV1()
        {
        }
    }
}
