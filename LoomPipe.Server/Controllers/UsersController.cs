using System;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/users")]
    [Authorize(Roles = "Admin")]
    public class UsersController : ControllerBase
    {
        private readonly IAppUserRepository _users;

        public UsersController(IAppUserRepository users)
        {
            _users = users;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            var all = await _users.GetAllAsync();
            var result = all.Select(u => new UserSummary
            {
                Id        = u.Id,
                Username  = u.Username,
                Role      = u.Role,
                IsActive  = u.IsActive,
                CreatedAt = u.CreatedAt,
            });
            return Ok(result);
        }

        [HttpPost]
        public async Task<IActionResult> Create([FromBody] CreateUserRequest req)
        {
            var existing = await _users.GetByUsernameAsync(req.Username);
            if (existing != null)
                return Conflict(new { message = "Username already exists." });

            var user = new AppUser
            {
                Username     = req.Username,
                PasswordHash = BCrypt.Net.BCrypt.HashPassword(req.Password),
                Role         = req.Role,
                CreatedAt    = DateTime.UtcNow,
                IsActive     = true,
            };
            await _users.AddAsync(user);
            return Ok(new UserSummary
            {
                Id        = user.Id,
                Username  = user.Username,
                Role      = user.Role,
                IsActive  = user.IsActive,
                CreatedAt = user.CreatedAt,
            });
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateUserRequest req)
        {
            var user = await _users.GetByIdAsync(id);
            if (user == null) return NotFound();

            user.Role     = req.Role;
            user.IsActive = req.IsActive;
            await _users.UpdateAsync(user);
            return NoContent();
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> Deactivate(int id)
        {
            var user = await _users.GetByIdAsync(id);
            if (user == null) return NotFound();

            user.IsActive = false;
            await _users.UpdateAsync(user);
            return NoContent();
        }
    }
}
