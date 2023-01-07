using System.Collections;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace Delta.Performance.Explorer
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
