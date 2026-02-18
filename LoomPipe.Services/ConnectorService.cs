using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using LoomPipe.Data.Database;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Services
{
    public class ConnectorService : IConnectorService
    {
        private readonly LoomPipeDbContext _context;

        public ConnectorService(LoomPipeDbContext context)
        {
            _context = context;
        }

        public async Task<Connector> CreateConnectorAsync(Connector connector)
        {
            _context.Connectors.Add(connector);
            await _context.SaveChangesAsync();
            return connector;
        }

        public async Task DeleteConnectorAsync(int id)
        {
            var connector = await _context.Connectors.FindAsync(id);
            if (connector != null)
            {
                _context.Connectors.Remove(connector);
                await _context.SaveChangesAsync();
            }
        }

        public async Task<Connector> GetConnectorAsync(int id)
        {
            return await _context.Connectors.FindAsync(id);
        }

        public async Task<IEnumerable<Connector>> GetConnectorsAsync()
        {
            return await _context.Connectors.ToListAsync();
        }

        public async Task UpdateConnectorAsync(Connector connector)
        {
            _context.Entry(connector).State = EntityState.Modified;
            await _context.SaveChangesAsync();
        }

        public async Task StartConnectorAsync(int id)
        {
            var connector = await _context.Connectors.FindAsync(id);
            if (connector == null) throw new System.InvalidOperationException($"Connector {id} not found.");
            connector.Status = Core.Enums.ConnectorStatus.Running;
            await _context.SaveChangesAsync();
        }

        public async Task StopConnectorAsync(int id)
        {
            var connector = await _context.Connectors.FindAsync(id);
            if (connector == null) throw new System.InvalidOperationException($"Connector {id} not found.");
            connector.Status = Core.Enums.ConnectorStatus.Stopped;
            await _context.SaveChangesAsync();
        }
    }
}
