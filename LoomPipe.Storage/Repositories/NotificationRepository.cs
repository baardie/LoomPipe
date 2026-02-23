using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class NotificationRepository : INotificationRepository
    {
        private readonly LoomPipeDbContext _db;

        public NotificationRepository(LoomPipeDbContext db)
        {
            _db = db;
        }

        public async Task AddAsync(Notification notification)
        {
            _db.Notifications.Add(notification);
            await _db.SaveChangesAsync();
        }

        public async Task<IEnumerable<Notification>> GetRecentAsync(int limit = 50) =>
            await _db.Notifications
                .OrderByDescending(n => n.CreatedAt)
                .Take(Math.Min(limit, 100))
                .ToListAsync();

        public async Task<int> GetUnreadCountAsync() =>
            await _db.Notifications.CountAsync(n => !n.IsRead);

        public async Task MarkReadAsync(int id)
        {
            var n = await _db.Notifications.FindAsync(id);
            if (n != null && !n.IsRead)
            {
                n.IsRead = true;
                await _db.SaveChangesAsync();
            }
        }

        public async Task MarkAllReadAsync()
        {
            await _db.Notifications
                .Where(n => !n.IsRead)
                .ExecuteUpdateAsync(s => s.SetProperty(n => n.IsRead, true));
        }
    }
}
