using System;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LoomPipe.Server.Auth
{
    /// <summary>
    /// Authenticates requests that carry a raw API key in the  X-Api-Key  header.
    /// The raw key is SHA-256-hashed and looked up in the database; on a match the
    /// calling user's claims are populated so existing [Authorize] attributes continue
    /// to work without modification.
    /// </summary>
    public class ApiKeyAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public const string SchemeName = "ApiKey";
        public const string HeaderName = "X-Api-Key";

        private readonly IApiKeyRepository _repo;

        public ApiKeyAuthHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder,
            IApiKeyRepository repo)
            : base(options, logger, encoder)
        {
            _repo = repo;
        }

        protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue(HeaderName, out var rawValues)
                || rawValues.Count == 0)
                return AuthenticateResult.NoResult();

            var raw  = rawValues[0]!.Trim();
            var hash = HashKey(raw);

            var apiKey = await _repo.GetByHashAsync(hash);
            if (apiKey == null)
                return AuthenticateResult.Fail("Invalid or inactive API key.");

            if (apiKey.ExpiresAt.HasValue && apiKey.ExpiresAt.Value < DateTime.UtcNow)
                return AuthenticateResult.Fail("API key has expired.");

            // Fire-and-forget: update LastUsedAt without blocking the request
            apiKey.LastUsedAt = DateTime.UtcNow;
            _ = _repo.UpdateAsync(apiKey);

            var user = apiKey.AppUser!;
            var claims = new[]
            {
                new Claim(ClaimTypes.NameIdentifier, user.Id.ToString()),
                new Claim(ClaimTypes.Name,           user.Username),
                new Claim(ClaimTypes.Role,           user.Role),
            };
            var identity  = new ClaimsIdentity(claims, SchemeName);
            var principal = new ClaimsPrincipal(identity);
            var ticket    = new AuthenticationTicket(principal, SchemeName);

            return AuthenticateResult.Success(ticket);
        }

        /// <summary>Returns the lowercase SHA-256 hex digest of the raw key string.</summary>
        public static string HashKey(string raw) =>
            Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(raw)));
    }
}
