using System.Security.Claims;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    /// <summary>
    /// Manages saved connection profiles.
    /// Credentials are encrypted at rest — EncryptedSecrets is NEVER returned from any endpoint.
    /// Access control: Admins see all profiles; Users see only profiles they have been granted access to.
    /// </summary>
    [ApiController]
    [Route("api/connections")]
    [Authorize]
    public class ConnectionProfilesController : ControllerBase
    {
        private readonly IConnectionProfileService _profileService;
        private readonly IConnectorFactory _connectorFactory;
        private readonly IUserConnectionPermissionRepository _permRepo;
        private readonly IAppUserRepository _userRepo;

        public ConnectionProfilesController(
            IConnectionProfileService profileService,
            IConnectorFactory connectorFactory,
            IUserConnectionPermissionRepository permRepo,
            IAppUserRepository userRepo)
        {
            _profileService = profileService;
            _connectorFactory = connectorFactory;
            _permRepo = permRepo;
            _userRepo = userRepo;
        }

        private int CallerUserId() =>
            int.TryParse(User.FindFirstValue(ClaimTypes.NameIdentifier), out var id) ? id : 0;

        private string CallerRole() =>
            User.FindFirstValue(ClaimTypes.Role) ?? "";

        // GET api/connections
        [HttpGet]
        public async Task<IEnumerable<ConnectionProfileSummary>> GetAll()
        {
            var profiles = await _profileService.GetAllAsync();
            if (CallerRole() == "User")
            {
                var allowed = (await _permRepo.GetProfileIdsForUserAsync(CallerUserId())).ToHashSet();
                profiles = profiles.Where(p => allowed.Contains(p.Id));
            }
            return profiles;
        }

        // GET api/connections/{id}
        [HttpGet("{id:int}")]
        public async Task<ActionResult<ConnectionProfileSummary>> GetById(int id)
        {
            if (CallerRole() == "User" && !await _permRepo.ExistsAsync(CallerUserId(), id))
                return Forbid();

            var summary = await _profileService.GetSummaryAsync(id);
            if (summary == null) return NotFound();
            return summary;
        }

        // POST api/connections
        [HttpPost]
        [Authorize(Roles = "Admin")]
        public async Task<ActionResult<ConnectionProfileSummary>> Create([FromBody] CreateConnectionProfileDto dto)
        {
            if (string.IsNullOrWhiteSpace(dto.Name))
                return BadRequest("Name is required.");
            if (string.IsNullOrWhiteSpace(dto.Provider))
                return BadRequest("Provider is required.");

            var summary = await _profileService.CreateAsync(dto);
            return CreatedAtAction(nameof(GetById), new { id = summary.Id }, summary);
        }

        // PUT api/connections/{id}
        [HttpPut("{id:int}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateConnectionProfileDto dto)
        {
            try
            {
                await _profileService.UpdateAsync(id, dto);
                return NoContent();
            }
            catch (InvalidOperationException)
            {
                return NotFound();
            }
        }

        // DELETE api/connections/{id}
        [HttpDelete("{id:int}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> Delete(int id)
        {
            await _profileService.DeleteAsync(id);
            return NoContent();
        }

        // POST api/connections/{id}/test
        [HttpPost("{id:int}/test")]
        public async Task<ActionResult<ConnectionTestResult>> Test(int id)
        {
            if (CallerRole() == "User" && !await _permRepo.ExistsAsync(CallerUserId(), id))
                return Forbid();

            var summary = await _profileService.GetSummaryAsync(id);
            if (summary == null) return NotFound();

            string connectionString;
            try
            {
                connectionString = await _profileService.BuildConnectionStringAsync(id);
            }
            catch (Exception ex)
            {
                return Ok(new ConnectionTestResult { Success = false, ErrorMessage = ex.Message, ElapsedMs = 0 });
            }

            var result = await _connectorFactory.TestConnectionAsync(summary.Provider, connectionString);
            await _profileService.RecordTestResultAsync(id, result.Success);
            return Ok(result);
        }

        // ── Permission management (Admin only) ──────────────────────────────────

        // GET api/connections/{id}/users
        [HttpGet("{id:int}/users")]
        [Authorize(Roles = "Admin")]
        public async Task<ActionResult<IEnumerable<int>>> GetPermittedUsers(int id)
            => Ok(await _permRepo.GetUserIdsForProfileAsync(id));

        // POST api/connections/{id}/users/{userId}
        [HttpPost("{id:int}/users/{userId:int}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> GrantAccess(int id, int userId)
        {
            var user = await _userRepo.GetByIdAsync(userId);
            if (user == null) return NotFound("User not found.");
            await _permRepo.AddAsync(userId, id);
            return NoContent();
        }

        // DELETE api/connections/{id}/users/{userId}
        [HttpDelete("{id:int}/users/{userId:int}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> RevokeAccess(int id, int userId)
        {
            await _permRepo.RemoveAsync(userId, id);
            return NoContent();
        }
    }
}
