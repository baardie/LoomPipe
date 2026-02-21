using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using System.Threading.Tasks;

namespace LoomPipe.Storage.Repositories
{
    public class SmtpSettingsRepository : ISmtpSettingsRepository
    {
        private readonly LoomPipeDbContext _context;

        public SmtpSettingsRepository(LoomPipeDbContext context)
        {
            _context = context;
        }

        public async Task<SmtpSettings?> GetAsync()
            => await _context.SmtpSettings.FindAsync(1);

        public async Task SaveAsync(SmtpSettings settings)
        {
            settings.Id = 1;
            var existing = await _context.SmtpSettings.FindAsync(1);
            if (existing == null)
                _context.SmtpSettings.Add(settings);
            else
                _context.Entry(existing).CurrentValues.SetValues(settings);

            await _context.SaveChangesAsync();
        }
    }
}
