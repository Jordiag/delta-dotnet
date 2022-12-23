namespace Delta.DeltaStructure.Base
{
    abstract class IndexedFile : NamedFile
    {
        internal long Index { get; }

        protected IndexedFile(long index, string name)
            : base(name) => Index = index;
    }
}
