using LoomPipe.Core.Entities;
using System.Threading.Tasks;

namespace LoomPipe.Storage.Interfaces
{
    public interface ISystemSettingsRepository
    {
        Task<SystemSettings> GetAsync();
        Task SaveAsync(SystemSettings settings);
    }
}
