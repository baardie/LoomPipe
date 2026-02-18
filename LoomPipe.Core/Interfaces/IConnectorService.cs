using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Core.Interfaces
{
    public interface IConnectorService
    {
        Task<IEnumerable<Connector>> GetConnectorsAsync();
        Task<Connector> GetConnectorAsync(int id);
        Task<Connector> CreateConnectorAsync(Connector connector);
        Task UpdateConnectorAsync(Connector connector);
        Task DeleteConnectorAsync(int id);
        Task StartConnectorAsync(int id);
        Task StopConnectorAsync(int id);
    }
}
