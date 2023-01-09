using Parquet.Data.Rows;

namespace Delta.Common
{
    /// <summary>
    /// Special comparer for row parquet.net.
    /// </summary>
    public class RowComparer : IEqualityComparer<Row>
    {
        /// <summary>
        /// Hash generator for row.
        /// </summary>
        /// <param name="row">Row object instance.</param>
        /// <returns></returns>
        public int GetHashCode(Row? row)
        {
            if(row != null)
            {
                unchecked
                {
                    int hash = 17;

                    foreach(object? item in row.Values)
                    {
                        hash = (hash * 23) + ((item != null) ? item.GetHashCode() : 0);
                    }

                    return hash;
                }
            }

            return 0;
        }

        /// <summary>
        /// Equal comparer.
        /// </summary>
        /// <param name="firstRow">First element to compare.</param>
        /// <param name="secondRow">Second element to compare.</param>
        /// <returns></returns>
        public bool Equals(Row? firstRow, Row? secondRow)
        {
            if(ReferenceEquals(firstRow, secondRow))
            {
                return true;
            }

            if(firstRow != null && secondRow != null &&
                (firstRow.Values.Length == secondRow.Values.Length))
            {
                for(int i = 0; i < firstRow.Values.Length; i++)
                {
                    if(!Equals(firstRow.Values[i], secondRow.Values[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }
    }
}

