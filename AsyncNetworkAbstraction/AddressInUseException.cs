using System.Runtime.Serialization;

namespace AsyncNetworkAbstraction
{
    [Serializable]
    internal class AddressInUseException : Exception
    {
        public AddressInUseException()
        {
        }

        public AddressInUseException(string? message) : base(message)
        {
        }

        public AddressInUseException(string? message, Exception? innerException) : base(message, innerException)
        {
        }

        protected AddressInUseException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}