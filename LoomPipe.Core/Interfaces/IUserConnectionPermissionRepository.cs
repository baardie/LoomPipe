using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Core.Interfaces
{
    public interface IUserConnectionPermissionRepository
    {
        Task<IEnumerable<int>> GetUserIdsForProfileAsync(int connectionProfileId);
        Task<IEnumerable<int>> GetProfileIdsForUserAsync(int userId);
        Task<bool> ExistsAsync(int userId, int connectionProfileId);
        Task AddAsync(int userId, int connectionProfileId);
        Task RemoveAsync(int userId, int connectionProfileId);
    }
}
