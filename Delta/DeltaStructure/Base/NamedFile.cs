namespace Delta.DeltaStructure.Base
{
    abstract class NamedFile
    {
        internal string Name { get; }

        protected NamedFile(string name) => Name = name;
    }
}
