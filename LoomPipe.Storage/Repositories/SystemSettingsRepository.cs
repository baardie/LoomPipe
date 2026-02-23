using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class SystemSettingsRepository : ISystemSettingsRepository
    {
        private readonly LoomPipeDbContext _db;

        public SystemSettingsRepository(LoomPipeDbContext db)
        {
            _db = db;
        }

        public async Task<SystemSettings> GetAsync()
        {
            var settings = await _db.SystemSettings.FirstOrDefaultAsync(s => s.Id == 1);
            return settings ?? new SystemSettings();
        }

        public async Task SaveAsync(SystemSettings settings)
        {
            settings.Id = 1;
            var existing = await _db.SystemSettings.FirstOrDefaultAsync(s => s.Id == 1);
            if (existing == null)
                _db.SystemSettings.Add(settings);
            else
            {
                existing.FailedRunRetentionDays = settings.FailedRunRetentionDays;
                _db.SystemSettings.Update(existing);
            }
            await _db.SaveChangesAsync();
        }
    }
}
