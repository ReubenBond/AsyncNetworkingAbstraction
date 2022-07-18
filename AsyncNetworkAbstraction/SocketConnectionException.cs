using System.Runtime.Serialization;

namespace AsyncNetworkAbstraction
{
    [Serializable]
    internal class SocketConnectionException : Exception
    {
        public SocketConnectionException()
        {
        }

        public SocketConnectionException(string? message) : base(message)
        {
        }

        public SocketConnectionException(string? message, Exception? innerException) : base(message, innerException)
        {
        }

        protected SocketConnectionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}