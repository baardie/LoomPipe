using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/auth")]
    public class AuthController : ControllerBase
    {
        private readonly IAppUserRepository _users;
        private readonly IConfiguration _config;

        public AuthController(IAppUserRepository users, IConfiguration config)
        {
            _users  = users;
            _config = config;
        }

        [AllowAnonymous]
        [HttpPost("login")]
        public async Task<IActionResult> Login([FromBody] LoginRequest req)
        {
            var user = await _users.GetByUsernameAsync(req.Username);
            if (user == null || !user.IsActive || !BCrypt.Net.BCrypt.Verify(req.Password, user.PasswordHash))
                return Unauthorized(new { message = "Invalid username or password." });

            var jwtSection  = _config.GetSection("Jwt");
            var secretKey   = jwtSection["SecretKey"] ?? string.Empty;
            var expiryHours = int.TryParse(jwtSection["ExpiryHours"], out var h) ? h : 8;
            var expiresAt   = DateTime.UtcNow.AddHours(expiryHours);

            var claims = new[]
            {
                new Claim(JwtRegisteredClaimNames.Sub,  user.Id.ToString()),
                new Claim(ClaimTypes.NameIdentifier,    user.Id.ToString()),
                new Claim(ClaimTypes.Name,              user.Username),
                new Claim(ClaimTypes.Role,              user.Role),
            };

            var key   = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secretKey));
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
            var token = new JwtSecurityToken(
                issuer:             jwtSection["Issuer"],
                audience:           jwtSection["Audience"],
                claims:             claims,
                expires:            expiresAt,
                signingCredentials: creds);

            return Ok(new LoginResponse
            {
                Token     = new JwtSecurityTokenHandler().WriteToken(token),
                Username  = user.Username,
                Role      = user.Role,
                ExpiresAt = expiresAt,
            });
        }

        [Authorize]
        [HttpGet("me")]
        public IActionResult Me()
        {
            var idStr = User.FindFirstValue(ClaimTypes.NameIdentifier);
            _ = int.TryParse(idStr, out var id);
            return Ok(new CurrentUserResponse
            {
                Id       = id,
                Username = User.Identity?.Name ?? string.Empty,
                Role     = User.FindFirstValue(ClaimTypes.Role) ?? string.Empty,
            });
        }
    }
}
