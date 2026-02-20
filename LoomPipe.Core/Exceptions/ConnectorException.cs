using System;

namespace LoomPipe.Core.Exceptions
{
    /// <summary>
    /// Thrown by source readers and destination writers when an I/O operation fails.
    /// </summary>
    public class ConnectorException : Exception
    {
        /// <summary>The connector type string (e.g. "sqlserver", "csv", "rest") that raised this error.</summary>
        public string? ConnectorType { get; }

        public ConnectorException(string message, Exception innerException, string? connectorType = null)
            : base(message, innerException)
        {
            ConnectorType = connectorType;
        }
    }
}
