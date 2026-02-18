using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LoomPipe.Storage.Interfaces
{
    public interface IDataSourceConfigRepository
    {
        Task<IEnumerable<DataSourceConfig>> GetAllAsync();
        Task<DataSourceConfig?> GetByIdAsync(int id);
        Task AddAsync(DataSourceConfig config);
        Task UpdateAsync(DataSourceConfig config);
        Task DeleteAsync(int id);
    }
}
