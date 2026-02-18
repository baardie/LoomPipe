using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class AppUserRepository : IAppUserRepository
    {
        private readonly LoomPipeDbContext _db;

        public AppUserRepository(LoomPipeDbContext db)
        {
            _db = db;
        }

        public Task<AppUser?> GetByUsernameAsync(string username) =>
            _db.AppUsers.FirstOrDefaultAsync(u => u.Username == username);

        public Task<AppUser?> GetByIdAsync(int id) =>
            _db.AppUsers.FirstOrDefaultAsync(u => u.Id == id);

        public async Task<IEnumerable<AppUser>> GetAllAsync() =>
            await _db.AppUsers.ToListAsync();

        public async Task AddAsync(AppUser user)
        {
            _db.AppUsers.Add(user);
            await _db.SaveChangesAsync();
        }

        public async Task UpdateAsync(AppUser user)
        {
            _db.AppUsers.Update(user);
            await _db.SaveChangesAsync();
        }

        public Task<bool> AnyAsync() =>
            _db.AppUsers.AnyAsync();
    }
}
