using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class ConnectionProfileRepository : IConnectionProfileRepository
    {
        private readonly LoomPipeDbContext _context;

        public ConnectionProfileRepository(LoomPipeDbContext context)
        {
            _context = context;
        }

        public async Task<IEnumerable<ConnectionProfile>> GetAllAsync()
            => await _context.ConnectionProfiles.ToListAsync();

        public async Task<ConnectionProfile?> GetByIdAsync(int id)
            => await _context.ConnectionProfiles.FindAsync(id);

        public async Task AddAsync(ConnectionProfile profile)
        {
            await _context.ConnectionProfiles.AddAsync(profile);
            await _context.SaveChangesAsync();
        }

        public async Task UpdateAsync(ConnectionProfile profile)
        {
            _context.ConnectionProfiles.Update(profile);
            await _context.SaveChangesAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var profile = await _context.ConnectionProfiles.FindAsync(id);
            if (profile != null)
            {
                _context.ConnectionProfiles.Remove(profile);
                await _context.SaveChangesAsync();
            }
        }
    }
}
