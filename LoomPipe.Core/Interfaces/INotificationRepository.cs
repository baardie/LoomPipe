using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;

namespace LoomPipe.Core.Interfaces
{
    public interface INotificationRepository
    {
        Task AddAsync(Notification notification);
        Task<IEnumerable<Notification>> GetRecentAsync(int limit = 50);
        Task<int> GetUnreadCountAsync();
        Task MarkReadAsync(int id);
        Task MarkAllReadAsync();
    }
}
