﻿using System.Runtime.Serialization;

namespace Orleans.Networking;

[Serializable]
public class ConnectionAbortedException : Exception
{
    public ConnectionAbortedException()
    {
    }

    public ConnectionAbortedException(string? message) : base(message)
    {
    }

    public ConnectionAbortedException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    protected ConnectionAbortedException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
}