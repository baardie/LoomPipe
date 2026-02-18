using LoomPipe.Core.Interfaces;
using System.Threading;
using System.Threading.Tasks;

namespace LoomPipe.Connectors
{
    public class WebhookConnector : IConnector
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            // Implementation for starting the webhook listener
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            // Implementation for stopping the webhook listener
            return Task.CompletedTask;
        }
    }
}
