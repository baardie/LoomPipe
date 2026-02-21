using System;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Server.Auth;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/apikeys")]
    [Authorize]
    public class ApiKeysController : ControllerBase
    {
        private readonly IApiKeyRepository _repo;

        public ApiKeysController(IApiKeyRepository repo) => _repo = repo;

        private int CurrentUserId =>
            int.Parse(User.FindFirstValue(ClaimTypes.NameIdentifier)!);

        // GET /api/apikeys — list caller's keys (no hashes exposed)
        [HttpGet]
        public async Task<IActionResult> List()
        {
            var keys = await _repo.GetByUserAsync(CurrentUserId);
            var result = keys.Select(k => new
            {
                k.Id,
                k.Name,
                k.IsActive,
                k.CreatedAt,
                k.LastUsedAt,
                k.ExpiresAt,
            });
            return Ok(result);
        }

        // POST /api/apikeys — create a new key; returns the raw key ONE TIME
        [HttpPost]
        public async Task<IActionResult> Create([FromBody] CreateApiKeyRequest req)
        {
            var rawKey = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
            var hash   = ApiKeyAuthHandler.HashKey(rawKey);

            var key = new ApiKey
            {
                AppUserId = CurrentUserId,
                Name      = req.Name,
                KeyHash   = hash,
                IsActive  = true,
                CreatedAt = DateTime.UtcNow,
                ExpiresAt = req.ExpiresAt,
            };

            await _repo.AddAsync(key);

            return Ok(new { key.Id, key.Name, key.CreatedAt, key.ExpiresAt, RawKey = rawKey });
        }

        // DELETE /api/apikeys/{id} — revoke (hard-delete) a key
        [HttpDelete("{id:int}")]
        public async Task<IActionResult> Revoke(int id)
        {
            var keys = await _repo.GetByUserAsync(CurrentUserId);
            if (keys.All(k => k.Id != id))
                return NotFound();

            await _repo.DeleteAsync(id);
            return NoContent();
        }
    }

    public record CreateApiKeyRequest(string Name, DateTime? ExpiresAt);
}
