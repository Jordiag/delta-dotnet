﻿namespace Delta.DeltaStructure.DeltaLog
{
    internal class LastCheckPointFile
    {
        internal string Name { get; }

        public LastCheckPointFile(string name)
        {
            Name = name;
        }
    }
}