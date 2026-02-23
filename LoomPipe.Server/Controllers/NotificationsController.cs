using System;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [Authorize]
    public class NotificationsController : ControllerBase
    {
        private readonly INotificationRepository _repo;

        public NotificationsController(INotificationRepository repo)
        {
            _repo = repo;
        }

        /// <summary>
        /// Returns the most recent notifications (default 50, max 100).
        /// </summary>
        [HttpGet]
        public async Task<IActionResult> GetRecent([FromQuery] int limit = 50)
        {
            limit = Math.Clamp(limit, 1, 100);
            var notifications = await _repo.GetRecentAsync(limit);

            var result = notifications.Select(n => new
            {
                n.Id,
                n.Type,
                n.Title,
                n.Message,
                n.PipelineId,
                n.CreatedAt,
                n.IsRead,
            });

            return Ok(result);
        }

        /// <summary>
        /// Returns the count of unread notifications.
        /// </summary>
        [HttpGet("unread-count")]
        public async Task<IActionResult> GetUnreadCount()
        {
            var count = await _repo.GetUnreadCountAsync();
            return Ok(new { count });
        }

        /// <summary>
        /// Marks a single notification as read.
        /// </summary>
        [HttpPatch("{id}/read")]
        public async Task<IActionResult> MarkRead(int id)
        {
            await _repo.MarkReadAsync(id);
            return NoContent();
        }

        /// <summary>
        /// Marks all notifications as read.
        /// </summary>
        [HttpPost("mark-all-read")]
        public async Task<IActionResult> MarkAllRead()
        {
            await _repo.MarkAllReadAsync();
            return NoContent();
        }
    }
}
