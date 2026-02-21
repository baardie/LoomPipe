using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;

namespace LoomPipe.Storage.Interfaces
{
    public interface IApiKeyRepository
    {
        Task<ApiKey?> GetByHashAsync(string keyHash);
        Task<IEnumerable<ApiKey>> GetByUserAsync(int userId);
        Task AddAsync(ApiKey key);
        Task UpdateAsync(ApiKey key);
        Task DeleteAsync(int id);
    }
}
