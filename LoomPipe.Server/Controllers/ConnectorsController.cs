using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ConnectorsController : ControllerBase
    {
        private readonly IConnectorService _connectorService;

        public ConnectorsController(IConnectorService connectorService)
        {
            _connectorService = connectorService;
        }

        [HttpGet]
        public async Task<IEnumerable<Connector>> Get()
        {
            return await _connectorService.GetConnectorsAsync();
        }

        [HttpGet("{id}")]
        public async Task<Connector> Get(int id)
        {
            return await _connectorService.GetConnectorAsync(id);
        }

        [HttpPost]
        public async Task<Connector> Post([FromBody] Connector connector)
        {
            return await _connectorService.CreateConnectorAsync(connector);
        }

        [HttpPut("{id}")]
        public async Task Put(int id, [FromBody] Connector connector)
        {
            await _connectorService.UpdateConnectorAsync(connector);
        }

        [HttpDelete("{id}")]
        public async Task Delete(int id)
        {
            await _connectorService.DeleteConnectorAsync(id);
        }

        [HttpPost("{id}/start")]
        public async Task Start(int id)
        {
            await _connectorService.StartConnectorAsync(id);
        }

        [HttpPost("{id}/stop")]
        public async Task Stop(int id)
        {
            await _connectorService.StopConnectorAsync(id);
        }
    }
}
