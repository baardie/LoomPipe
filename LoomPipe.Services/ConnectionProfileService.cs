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
                "csv"        => host ?? throw new InvalidOperationException("CSV file path (Host field) is required."),
                "rest"       => host ?? throw new InvalidOperationException("REST URL (Host field) is required."),
                "webhook"    => host ?? throw new InvalidOperationException("Webhook URL (Host field) is required."),
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
                "stripe"       => pw,
                "hubspot"      => pw,
                "shopify"      => JsonSerializer.Serialize(new { shopDomain = host, accessToken = pw }),
                "googlesheets" => JsonSerializer.Serialize(new { spreadsheetId = host ?? "", apiKey = user, accessToken = pw }),
                "s3"           => JsonSerializer.Serialize(new { bucket = Get("bucket", host ?? ""), accessKeyId = user, secretAccessKey = pw, region = Get("region", "us-east-1"), endpointUrl = Get("endpointUrl") }),

                // ── Simple token (return password as-is) ─────────────────────
                "pipedrive"    => pw,
                "chargebee"    => pw,
                "toggl"        => pw,
                "pagerduty"    => pw,
                "greenhouse"   => pw,
                "lever"        => pw,
                "close"        => pw,
                "copper"       => pw,
                "freshsales"   => pw,
                "recurly"      => pw,
                "brevo"        => pw,

                // ── Simple access token (return password) ────────────────────
                "slack"          => pw,
                "intercom"       => pw,
                "asana"          => pw,
                "monday"         => pw,
                "linear"         => pw,
                "notion"         => pw,
                "sendgrid"       => pw,
                "segment"        => pw,
                "klaviyo"        => pw,
                "webflow"        => pw,
                "typeform"       => pw,
                "surveymonkey"   => pw,
                "salesloft"      => pw,
                "sentry"         => pw,
                "reddit"         => pw,
                "outreach"       => pw,
                "pardot"         => pw,
                "snapchatads"    => pw,
                "apollo"         => pw,
                "gusto"          => pw,

                // ── JSON with domain/subdomain + token ───────────────────────
                "zendesk"        => JsonSerializer.Serialize(new { subdomain = host, accessToken = pw, email = user }),
                "jira"           => JsonSerializer.Serialize(new { domain = host, email = user, accessToken = pw }),
                "confluence"     => JsonSerializer.Serialize(new { domain = host, email = user, accessToken = pw }),
                "okta"           => JsonSerializer.Serialize(new { domain = host, accessToken = pw }),
                "freshdesk"      => JsonSerializer.Serialize(new { domain = host, accessToken = pw }),
                "servicenow"     => JsonSerializer.Serialize(new { instance = host, username = user, password = pw }),
                "bamboohr"       => JsonSerializer.Serialize(new { companyDomain = host, accessToken = pw }),
                "dynamics365"    => JsonSerializer.Serialize(new { orgUrl = host, accessToken = pw }),
                "zohocrm"        => pw,
                "netsuite"       => JsonSerializer.Serialize(new { accountId = host, accessToken = pw }),
                "salesforcemarketingcloud" => JsonSerializer.Serialize(new { subdomain = host, clientId = user, clientSecret = pw }),
                "sfcc"           => JsonSerializer.Serialize(new { host, accessToken = pw }),

                // ── JSON with credentials ────────────────────────────────────
                "salesforce"     => JsonSerializer.Serialize(new { instanceUrl = host, accessToken = pw }),
                "github"         => pw,
                "gitlab"         => pw,
                "bitbucket"      => pw,
                "airtable"       => pw,
                "woocommerce"    => JsonSerializer.Serialize(new { storeUrl = host, consumerKey = user, consumerSecret = pw }),
                "quickbooks"     => pw,
                "xero"           => pw,

                // ── Advertising / analytics ──────────────────────────────────
                "googleads"            => JsonSerializer.Serialize(new { accessToken = pw, developerToken = user }),
                "facebookads"          => pw,
                "linkedinads"          => pw,
                "googleanalytics"      => pw,
                "googlesearchconsole"  => pw,
                "tiktokads"            => pw,
                "instagram"            => pw,
                "youtube"              => pw,
                "twitter"              => pw,
                "bingads"              => JsonSerializer.Serialize(new { accessToken = pw, developerToken = user }),
                "microsoftads"         => JsonSerializer.Serialize(new { accessToken = pw, developerToken = user }),
                "pinterestads"         => pw,

                // ── Infrastructure tools ─────────────────────────────────────
                "datadog"        => JsonSerializer.Serialize(new { apiKey = pw, appKey = user, site = Get("site", "datadoghq.com") }),
                "mixpanel"       => JsonSerializer.Serialize(new { username = user, accessToken = pw }),
                "amplitude"      => JsonSerializer.Serialize(new { apiKey = user, secretKey = pw }),
                "marketo"        => JsonSerializer.Serialize(new { munchkinId = host, clientId = user, clientSecret = pw }),
                "twilio"         => JsonSerializer.Serialize(new { accountSid = user, authToken = pw }),
                "mailchimp"      => pw,
                "paypal"         => JsonSerializer.Serialize(new { clientId = user, clientSecret = pw }),
                "square"         => pw,
                "bigcommerce"    => JsonSerializer.Serialize(new { storeHash = host, accessToken = pw }),

                // ── Cloud / storage ──────────────────────────────────────────
                "gcs"            => JsonSerializer.Serialize(new { bucket = Get("bucket", host ?? ""), projectId = Get("projectId"), serviceAccountJson = saJson }),
                "azureblob"      => JsonSerializer.Serialize(new { connectionString = host, container = Get("container", db ?? "") }),
                "sftp"           => JsonSerializer.Serialize(new { host, port = port ?? 22, username = user, password = pw }),

                // ── Enterprise ───────────────────────────────────────────────
                "sap"            => JsonSerializer.Serialize(new { host, username = user, password = pw, sapClient = Get("sapClient", "100") }),
                "workday"        => JsonSerializer.Serialize(new { host, tenant = Get("tenant", db ?? ""), accessToken = pw }),
                "shopifyplus"    => JsonSerializer.Serialize(new { shopDomain = host, accessToken = pw }),

                // ── Databases ────────────────────────────────────────────────
                "elasticsearch"  => JsonSerializer.Serialize(new { host, port = port ?? 9200, username = user, password = pw }),
                "dynamodb"       => JsonSerializer.Serialize(new { accessKeyId = user, secretAccessKey = pw, region = Get("region", "us-east-1") }),
                "redis"          => JsonSerializer.Serialize(new { host, accessToken = pw, port = port ?? 6379 }),
                "cassandra"      => JsonSerializer.Serialize(new { host, port = port ?? 9042, keyspace = db, username = user, password = pw }),
                "clickhouse"     => JsonSerializer.Serialize(new { host, port = port ?? 8123, database = db, username = user, password = pw }),
                "databricks"     => JsonSerializer.Serialize(new { accessToken = pw, host, warehouseId = Get("warehouseId") }),
                "redshift"       => $"Host={host};Port={port ?? 5439};Database={db};Username={user};Password={pw}",
                "firebase"       => JsonSerializer.Serialize(new { projectId = Get("projectId", host ?? ""), accessToken = pw, collection = db }),

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
