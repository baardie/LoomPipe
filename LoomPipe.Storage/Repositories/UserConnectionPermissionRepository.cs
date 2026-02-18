using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class UserConnectionPermissionRepository : IUserConnectionPermissionRepository
    {
        private readonly LoomPipeDbContext _db;

        public UserConnectionPermissionRepository(LoomPipeDbContext db) => _db = db;

        public async Task<IEnumerable<int>> GetUserIdsForProfileAsync(int connectionProfileId)
            => await _db.UserConnectionPermissions
                .Where(p => p.ConnectionProfileId == connectionProfileId)
                .Select(p => p.UserId)
                .ToListAsync();

        public async Task<IEnumerable<int>> GetProfileIdsForUserAsync(int userId)
            => await _db.UserConnectionPermissions
                .Where(p => p.UserId == userId)
                .Select(p => p.ConnectionProfileId)
                .ToListAsync();

        public async Task<bool> ExistsAsync(int userId, int connectionProfileId)
            => await _db.UserConnectionPermissions
                .AnyAsync(p => p.UserId == userId && p.ConnectionProfileId == connectionProfileId);

        public async Task AddAsync(int userId, int connectionProfileId)
        {
            if (!await ExistsAsync(userId, connectionProfileId))
            {
                _db.UserConnectionPermissions.Add(new UserConnectionPermission
                {
                    UserId = userId,
                    ConnectionProfileId = connectionProfileId,
                });
                await _db.SaveChangesAsync();
            }
        }

        public async Task RemoveAsync(int userId, int connectionProfileId)
        {
            var entry = await _db.UserConnectionPermissions
                .FirstOrDefaultAsync(p => p.UserId == userId && p.ConnectionProfileId == connectionProfileId);
            if (entry != null)
            {
                _db.UserConnectionPermissions.Remove(entry);
                await _db.SaveChangesAsync();
            }
        }
    }
}
