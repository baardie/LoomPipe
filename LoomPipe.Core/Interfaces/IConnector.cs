using System.Threading;
using System.Threading.Tasks;

namespace LoomPipe.Core.Interfaces
{
    public interface IConnector
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);
    }
}
