using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class DataSourceConfigRepository : IDataSourceConfigRepository
    {
        private readonly LoomPipeDbContext _context;
        public DataSourceConfigRepository(LoomPipeDbContext context)
        {
            _context = context;
        }

        public async Task<IEnumerable<DataSourceConfig>> GetAllAsync()
        {
            return await _context.DataSourceConfigs.ToListAsync();
        }

        public async Task<DataSourceConfig?> GetByIdAsync(int id)
        {
            return await _context.DataSourceConfigs.FindAsync(id);
        }

        public async Task AddAsync(DataSourceConfig config)
        {
            await _context.DataSourceConfigs.AddAsync(config);
            await _context.SaveChangesAsync();
        }

        public async Task UpdateAsync(DataSourceConfig config)
        {
            _context.DataSourceConfigs.Update(config);
            await _context.SaveChangesAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var config = await _context.DataSourceConfigs.FindAsync(id);
            if (config != null)
            {
                _context.DataSourceConfigs.Remove(config);
                await _context.SaveChangesAsync();
            }
        }
    }
}
