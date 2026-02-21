using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class ApiKeyRepository : IApiKeyRepository
    {
        private readonly LoomPipeDbContext _db;

        public ApiKeyRepository(LoomPipeDbContext db) => _db = db;

        public Task<ApiKey?> GetByHashAsync(string keyHash) =>
            _db.ApiKeys.Include(k => k.AppUser)
                       .FirstOrDefaultAsync(k => k.KeyHash == keyHash && k.IsActive);

        public async Task<IEnumerable<ApiKey>> GetByUserAsync(int userId) =>
            await _db.ApiKeys.Where(k => k.AppUserId == userId).ToListAsync();

        public async Task AddAsync(ApiKey key)
        {
            _db.ApiKeys.Add(key);
            await _db.SaveChangesAsync();
        }

        public async Task UpdateAsync(ApiKey key)
        {
            _db.ApiKeys.Update(key);
            await _db.SaveChangesAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var key = await _db.ApiKeys.FindAsync(id);
            if (key != null)
            {
                _db.ApiKeys.Remove(key);
                await _db.SaveChangesAsync();
            }
        }
    }
}
