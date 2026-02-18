using LoomPipe.Core.DTOs;

namespace LoomPipe.Core.Interfaces
{
    public interface IConnectorFactory
    {
        ISourceReader CreateSourceReader(string type);
        IDestinationWriter CreateDestinationWriter(string type);

        /// <summary>
        /// Opens and immediately closes a connection to verify credentials.
        /// Used by the connection profile test endpoint.
        /// </summary>
        Task<ConnectionTestResult> TestConnectionAsync(string provider, string connectionString);
    }
}
