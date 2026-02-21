using LoomPipe.Core.Entities;
using System.Threading.Tasks;

namespace LoomPipe.Storage.Interfaces
{
    public interface ISmtpSettingsRepository
    {
        Task<SmtpSettings?> GetAsync();
        Task SaveAsync(SmtpSettings settings);
    }
}
