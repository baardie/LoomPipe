using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Storage.Interfaces
{
    public interface IPipelineRepository
    {
        Task<IEnumerable<Pipeline>> GetAllAsync();
        Task<Pipeline?> GetByIdAsync(int id);
        Task AddAsync(Pipeline pipeline);
        Task UpdateAsync(Pipeline pipeline);
        Task DeleteAsync(int id);
    }
}
