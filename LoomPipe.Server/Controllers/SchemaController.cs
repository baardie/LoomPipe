using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Server.Controllers
{
    public class DiscoverSourceRequest
    {
        public string Type { get; set; } = string.Empty;
        public string ConnectionString { get; set; } = string.Empty;
        public Dictionary<string, object>? Parameters { get; set; }
    }

    [ApiController]
    [Route("api/[controller]")]
    public class SchemaController : ControllerBase
    {
        private readonly IConnectorFactory _connectorFactory;

        public SchemaController(IConnectorFactory connectorFactory)
        {
            _connectorFactory = connectorFactory;
        }

        [HttpPost("source")]
        public async Task<ActionResult<IEnumerable<string>>> DiscoverSource([FromBody] DiscoverSourceRequest request)
        {
            if (request == null || string.IsNullOrWhiteSpace(request.Type))
                return BadRequest("Type is required.");

            ISourceReader reader;
            try
            {
                reader = _connectorFactory.CreateSourceReader(request.Type);
            }
            catch (System.NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }

            var config = new DataSourceConfig
            {
                Type = request.Type,
                ConnectionString = request.ConnectionString,
                Parameters = request.Parameters ?? new Dictionary<string, object>()
            };

            try
            {
                var schema = await reader.DiscoverSchemaAsync(config);
                return Ok(schema);
            }
            catch (System.Exception)
            {
                return StatusCode(500, "Schema discovery failed. Check server logs for details.");
            }
        }

        [HttpPost("resources")]
        public async Task<ActionResult<IEnumerable<string>>> ListResources([FromBody] DiscoverSourceRequest request)
        {
            if (request == null || string.IsNullOrWhiteSpace(request.Type))
                return BadRequest("Type is required.");

            ISourceReader reader;
            try
            {
                reader = _connectorFactory.CreateSourceReader(request.Type);
            }
            catch (System.NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }

            var config = new DataSourceConfig
            {
                Type = request.Type,
                ConnectionString = request.ConnectionString,
                Parameters = request.Parameters ?? new Dictionary<string, object>()
            };

            try
            {
                var resources = await reader.ListResourcesAsync(config);
                return Ok(resources);
            }
            catch (System.Exception)
            {
                return StatusCode(500, "Resource listing failed. Check server logs for details.");
            }
        }
    }
}
