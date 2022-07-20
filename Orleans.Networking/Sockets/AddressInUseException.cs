﻿using System.Runtime.Serialization;

namespace Orleans.Networking.Sockets;

[Serializable]
public class AddressInUseException : Exception
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