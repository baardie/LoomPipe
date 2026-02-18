#nullable enable
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace LoomPipe.Services
{
    public class ConnectionProfileService : IConnectionProfileService
    {
        private const string Purpose = "LoomPipe.ConnectionProfiles.v1";

        private readonly IConnectionProfileRepository _repo;
        private readonly IDataProtector _protector;
        private readonly ILogger<ConnectionProfileService> _logger;

        public ConnectionProfileService(
            IConnectionProfileRepository repo,
            IDataProtectionProvider dpProvider,
            ILogger<ConnectionProfileService> logger)
        {
            _repo = repo;
            _protector = dpProvider.CreateProtector(Purpose);
            _logger = logger;
        }

        // ── CRUD ──────────────────────────────────────────────────────────────

        public async Task<IEnumerable<ConnectionProfileSummary>> GetAllAsync()
        {
            var profiles = await _repo.GetAllAsync();
            return profiles.Select(ToSummary);
        }

        public async Task<ConnectionProfileSummary?> GetSummaryAsync(int id)
        {
            var profile = await _repo.GetByIdAsync(id);
            return profile == null ? null : ToSummary(profile);
        }

        public async Task<ConnectionProfileSummary> CreateAsync(CreateConnectionProfileDto dto)
        {
            var profile = new ConnectionProfile
            {
                Name           = dto.Name,
                Provider       = dto.Provider.ToLowerInvariant(),
                Host           = dto.Host,
                Port           = dto.Port,
                DatabaseName   = dto.DatabaseName,
                Username       = dto.Username,
                AdditionalConfig = dto.AdditionalConfig,
                EncryptedSecrets = EncryptSecrets(dto.Password, dto.ApiKey, dto.ServiceAccountJson),
                CreatedAt      = DateTime.UtcNow,
            };
            await _repo.AddAsync(profile);
            _logger.LogInformation("Created connection profile {Id} ({Name})", profile.Id, profile.Name);
            return ToSummary(profile);
        }

        public async Task UpdateAsync(int id, UpdateConnectionProfileDto dto)
        {
            var profile = await _repo.GetByIdAsync(id)
                ?? throw new InvalidOperationException($"Connection profile {id} not found.");

            profile.Name           = dto.Name;
            profile.Provider       = dto.Provider.ToLowerInvariant();
            profile.Host           = dto.Host;
            profile.Port           = dto.Port;
            profile.DatabaseName   = dto.DatabaseName;
            profile.Username       = dto.Username;
            profile.AdditionalConfig = dto.AdditionalConfig;

            // Only re-encrypt if at least one secret field was supplied
            if (dto.Password != null || dto.ApiKey != null || dto.ServiceAccountJson != null)
            {
                // Preserve existing encrypted values for fields not being updated
                var existing = DecryptSecrets(profile.EncryptedSecrets);
                var newPw    = dto.Password             ?? existing.GetValueOrDefault("password") as string;
                var newKey   = dto.ApiKey               ?? existing.GetValueOrDefault("apiKey") as string;
                var newSa    = dto.ServiceAccountJson   ?? existing.GetValueOrDefault("serviceAccountJson") as string;
                profile.EncryptedSecrets = EncryptSecrets(newPw, newKey, newSa);
            }

            await _repo.UpdateAsync(profile);
        }

        public async Task DeleteAsync(int id)
        {
            await _repo.DeleteAsync(id);
            _logger.LogInformation("Deleted connection profile {Id}", id);
        }

        // ── Connection string builder ──────────────────────────────────────────

        public async Task<string> BuildConnectionStringAsync(int profileId)
        {
            var profile = await _repo.GetByIdAsync(profileId)
                ?? throw new InvalidOperationException($"Connection profile {profileId} not found.");

            var secrets = DecryptSecrets(profile.EncryptedSecrets);
            var pw      = secrets.GetValueOrDefault("password") as string ?? string.Empty;
            var apiKey  = secrets.GetValueOrDefault("apiKey") as string ?? string.Empty;
            var saJson  = secrets.GetValueOrDefault("serviceAccountJson") as string ?? string.Empty;

            // Extra config (non-sensitive)
            var extra = string.IsNullOrWhiteSpace(profile.AdditionalConfig) || profile.AdditionalConfig == "{}"
                ? new Dictionary<string, JsonElement>()
                : JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(profile.AdditionalConfig)
                  ?? new Dictionary<string, JsonElement>();

            string Get(string key, string fallback = "") =>
                extra.TryGetValue(key, out var v) ? v.GetString() ?? fallback : fallback;

            var host = profile.Host;
            var port = profile.Port;
            var db   = profile.DatabaseName;
            var user = profile.Username;

            return profile.Provider switch
            {
                "sqlserver"  => $"Server={host},{port};Database={db};User Id={user};Password={pw};TrustServerCertificate=True",
                "postgresql" => $"Host={host};Port={port};Database={db};Username={user};Password={pw}",
                "mysql"      => $"Server={host};Port={port};Database={db};Uid={user};Pwd={pw}",
                "oracle"     => $"Data Source={host}:{port}/{db};User Id={user};Password={pw}",
                "mongodb"    => string.IsNullOrEmpty(user)
                                    ? $"mongodb://{host}:{port}/{db}"
                                    : $"mongodb://{Uri.EscapeDataString(user)}:{Uri.EscapeDataString(pw)}@{host}:{port}/{db}",
                "neo4j"      => JsonSerializer.Serialize(new { uri = $"bolt://{host}:{port ?? 7687}", user, password = pw }),
                "snowflake"  => $"account={Get("account")};user={user};password={pw};warehouse={Get("warehouse")};db={db};schema={Get("schema")}",
                "bigquery"   => JsonSerializer.Serialize(new { projectId = Get("projectId"), dataset = Get("dataset"), serviceAccountJson = saJson }),
                "pinecone"   => JsonSerializer.Serialize(new { apiKey, indexName = Get("indexName"), environment = Get("environment") }),
                "milvus"     => JsonSerializer.Serialize(new { host, port, collection = Get("collection"), user, password = pw }),
                _            => throw new NotSupportedException($"Provider '{profile.Provider}' is not supported.")
            };
        }

        public async Task<ConnectionTestResult> TestConnectionAsync(int id)
        {
            // Actual connection test is delegated to ConnectorFactory in the controller.
            // This overload resolves the profile and returns its summary — kept for interface completeness.
            var profile = await _repo.GetByIdAsync(id);
            if (profile == null)
                return new ConnectionTestResult { Success = false, ErrorMessage = "Profile not found.", ElapsedMs = 0 };

            // Mark result after test (called by controller after it runs the real test)
            return new ConnectionTestResult { Success = false, ErrorMessage = "Call the controller endpoint which uses IConnectorFactory.", ElapsedMs = 0 };
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private string EncryptSecrets(string? password, string? apiKey, string? serviceAccountJson)
        {
            var secrets = new Dictionary<string, string?>(3)
            {
                ["password"]           = password,
                ["apiKey"]             = apiKey,
                ["serviceAccountJson"] = serviceAccountJson,
            };
            var json = JsonSerializer.Serialize(secrets);
            return _protector.Protect(json);
        }

        private Dictionary<string, object?> DecryptSecrets(string encrypted)
        {
            if (string.IsNullOrEmpty(encrypted))
                return new Dictionary<string, object?>();
            try
            {
                var json = _protector.Unprotect(encrypted);
                return JsonSerializer.Deserialize<Dictionary<string, object?>>(json)
                       ?? new Dictionary<string, object?>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to decrypt connection profile secrets.");
                return new Dictionary<string, object?>();
            }
        }

        /// <summary>
        /// Records the test result on the profile entity (called by controller after testing).
        /// </summary>
        public async Task RecordTestResultAsync(int id, bool success)
        {
            var profile = await _repo.GetByIdAsync(id);
            if (profile == null) return;
            profile.LastTestedAt = DateTime.UtcNow;
            profile.LastTestSucceeded = success;
            await _repo.UpdateAsync(profile);
        }

        private static ConnectionProfileSummary ToSummary(ConnectionProfile p) => new()
        {
            Id               = p.Id,
            Name             = p.Name,
            Provider         = p.Provider,
            Host             = p.Host,
            Port             = p.Port,
            DatabaseName     = p.DatabaseName,
            Username         = p.Username,
            AdditionalConfig = p.AdditionalConfig,
            CreatedAt        = p.CreatedAt,
            LastTestedAt     = p.LastTestedAt,
            LastTestSucceeded = p.LastTestSucceeded,
        };
    }
}
