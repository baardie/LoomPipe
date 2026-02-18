using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;

namespace LoomPipe.Core.Interfaces
{
    public interface IAppUserRepository
    {
        Task<AppUser?> GetByUsernameAsync(string username);
        Task<AppUser?> GetByIdAsync(int id);
        Task<IEnumerable<AppUser>> GetAllAsync();
        Task AddAsync(AppUser user);
        Task UpdateAsync(AppUser user);
        Task<bool> AnyAsync();
    }
}
