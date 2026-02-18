using LoomPipe.Core.Entities;

namespace LoomPipe.Storage.Interfaces
{
    public interface IConnectionProfileRepository
    {
        Task<IEnumerable<ConnectionProfile>> GetAllAsync();
        Task<ConnectionProfile?> GetByIdAsync(int id);
        Task AddAsync(ConnectionProfile profile);
        Task UpdateAsync(ConnectionProfile profile);
        Task DeleteAsync(int id);
    }
}
