using System.Runtime.Serialization;

namespace Delta.Common
{
    /// <summary>
    /// Custom Delta Lake Table exception.
    /// </summary>
    [Serializable]
    public class DeltaException : Exception
    {
        private const string DefaultMessage = "Delta Lake Table error.";

        /// <summary>
        /// Creates an instance
        /// </summary>
        public DeltaException() : base(DefaultMessage)
        {
        }

        /// <summary>
        /// Creates an instance
        /// </summary>
        /// <param name="message"></param>
        public DeltaException(string message) : base(message)
        {
        }

        /// <summary>
        /// Creates an instance
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public DeltaException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Creates an instance
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected DeltaException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
