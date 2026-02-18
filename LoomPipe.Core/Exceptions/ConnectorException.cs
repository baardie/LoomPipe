using System;

namespace LoomPipe.Core.Exceptions
{
    public class ConnectorException : Exception
    {
        public ConnectorException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
