#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from the Zendesk Support API v2.
    ///
    /// Parameters:
    ///   accessToken  — Zendesk API token or OAuth Bearer token
    ///   subdomain    — Zendesk account subdomain (e.g. "mycompany" for mycompany.zendesk.com)
    ///   resource     — tickets, users, organizations, groups, ticket_fields, ticket_forms,
    ///                  brands, views, macros, triggers, automations, sla_policies, satisfaction_ratings
    ///   email        — optional agent email for Basic auth (email/token:{apiToken})
    ///   startDate    — optional ISO date for incremental ticket export (unix timestamp)
    ///
    /// ConnectionString JSON: {"subdomain":"...","accessToken":"...","email":"..."}
    /// </summary>
    public class ZendeskReader : ISourceReader
    {
        private const int PageSize = 100;
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "tickets", "users", "organizations", "groups",
            "ticket_fields", "ticket_forms", "brands", "views",
            "macros", "triggers", "automations", "sla_policies",
            "satisfaction_ratings"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ZendeskReader> _logger;

        public ZendeskReader(HttpClient httpClient, ILogger<ZendeskReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var subdomain   = ResolveSubdomain(config);
            var accessToken = ResolveAccessToken(config);
            var email       = ResolveEmail(config);
            var startDate   = GetStringParam(config.Parameters, "startDate");

            _logger.LogInformation("Zendesk: reading resource '{Resource}' from subdomain '{Subdomain}'.",
                resource, subdomain);

            try
            {
                // Incremental export for tickets when watermark or startDate is provided.
                if (resource == "tickets" &&
                    (!string.IsNullOrEmpty(watermarkValue) || !string.IsNullOrEmpty(startDate)))
                {
                    var since = watermarkValue ?? startDate!;
                    return await ReadIncrementalTicketsAsync(subdomain, accessToken, email, since);
                }

                return await ReadFullAsync(subdomain, accessToken, email, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zendesk: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Failed to read Zendesk resource '{resource}': {ex.Message}", ex, "zendesk");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Zendesk: discovering schema for '{Resource}'.", resource);

            try
            {
                // Fetch a single page and inspect the first record's keys.
                var records = await ReadFullAsync(
                    ResolveSubdomain(config),
                    ResolveAccessToken(config),
                    ResolveEmail(config),
                    resource,
                    maxPages: 1);

                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zendesk: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Failed to discover Zendesk schema for '{resource}': {ex.Message}", ex, "zendesk");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Zendesk: dry run preview for '{Resource}' (sample={SampleSize}).",
                resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(
                    ResolveSubdomain(config),
                    ResolveAccessToken(config),
                    ResolveEmail(config),
                    resource,
                    maxPages: 1);

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zendesk: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Zendesk dry run preview failed for '{resource}': {ex.Message}", ex, "zendesk");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based pagination) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string subdomain, string accessToken, string? email,
            string resource, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var baseUrl = $"https://{subdomain}.zendesk.com/api/v2";
            var dataKey = ResolveDataKey(resource);
            var url     = $"{baseUrl}/{ResolveEndpointPath(resource)}?page[size]={PageSize}";
            int page    = 0;

            do
            {
                var json = await SendWithRetryAsync(url, accessToken, email);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse records from the appropriate data key.
                if (root.TryGetProperty(dataKey, out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        results.Add(ParseObject(element));
                    }
                }

                // Cursor-based pagination via "meta.after_cursor" and "links.next".
                url = null!;
                if (root.TryGetProperty("meta", out var meta)
                    && meta.TryGetProperty("has_more", out var hasMore)
                    && hasMore.ValueKind == JsonValueKind.True
                    && root.TryGetProperty("links", out var links)
                    && links.TryGetProperty("next", out var next)
                    && next.ValueKind == JsonValueKind.String)
                {
                    url = next.GetString()!;
                }

                page++;
            }
            while (url != null && page < maxPages);

            _logger.LogInformation("Zendesk: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Incremental ticket export ────────────────────────────────────────

        private async Task<List<object>> ReadIncrementalTicketsAsync(
            string subdomain, string accessToken, string? email, string sinceValue)
        {
            var unixTimestamp = ConvertToUnixTimestamp(sinceValue);
            var baseUrl = $"https://{subdomain}.zendesk.com/api/v2";
            var url = $"{baseUrl}/incremental/tickets.json?start_time={unixTimestamp}";
            var results = new List<object>();

            do
            {
                var json = await SendWithRetryAsync(url, accessToken, email);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("tickets", out var tickets) && tickets.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in tickets.EnumerateArray())
                    {
                        results.Add(ParseObject(element));
                    }
                }

                // Incremental exports use "end_of_stream" and "next_page".
                url = null!;
                if (root.TryGetProperty("end_of_stream", out var eos)
                    && eos.ValueKind == JsonValueKind.False
                    && root.TryGetProperty("next_page", out var nextPage)
                    && nextPage.ValueKind == JsonValueKind.String)
                {
                    url = nextPage.GetString()!;
                }
            }
            while (url != null);

            _logger.LogInformation("Zendesk: incremental export returned {Count} tickets.", results.Count);
            return results;
        }

        // ── HTTP with retry / rate-limit handling ────────────────────────────

        private async Task<string> SendWithRetryAsync(string url, string accessToken, string? email)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    // Zendesk returns Retry-After header (in seconds).
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds
                                     ?? response.Headers.RetryAfter?.Date?.Subtract(DateTimeOffset.UtcNow).TotalSeconds
                                     ?? 5.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("Zendesk rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Zendesk API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "zendesk");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("Zendesk API error {StatusCode} for {Url}: {Body}",
                        (int)response.StatusCode, url, errorBody);

                    throw new ConnectorException(
                        $"Zendesk API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "zendesk");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "Zendesk request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "zendesk");
        }

        // ── JSON parsing ─────────────────────────────────────────────────────

        private static IDictionary<string, object> ParseObject(JsonElement element)
        {
            IDictionary<string, object> expando = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                expando[prop.Name] = prop.Value.ValueKind switch
                {
                    JsonValueKind.String => (object)(prop.Value.GetString() ?? string.Empty),
                    JsonValueKind.Number => prop.Value.TryGetInt64(out var l) ? l : prop.Value.GetDouble(),
                    JsonValueKind.True   => true,
                    JsonValueKind.False  => false,
                    JsonValueKind.Null   => string.Empty,
                    JsonValueKind.Object => prop.Value.GetRawText(),
                    JsonValueKind.Array  => prop.Value.GetRawText(),
                    _                    => prop.Value.ToString()
                };
            }

            return expando;
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        /// <summary>
        /// Applies authentication to the request.
        /// If <paramref name="email"/> is provided, uses Basic auth (email/token:{apiToken}).
        /// Otherwise, uses Bearer token auth.
        /// </summary>
        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string? email)
        {
            if (!string.IsNullOrEmpty(email))
            {
                // Zendesk Basic auth: {email}/token:{apiToken}
                var credentials = $"{email}/token:{accessToken}";
                var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", encoded);
            }
            else
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            }
        }

        // ── Endpoint / data-key mapping ──────────────────────────────────────

        /// <summary>
        /// Maps the resource name to the Zendesk API endpoint path.
        /// </summary>
        private static string ResolveEndpointPath(string resource) => resource switch
        {
            "tickets"              => "tickets.json",
            "users"                => "users.json",
            "organizations"        => "organizations.json",
            "groups"               => "groups.json",
            "ticket_fields"        => "ticket_fields.json",
            "ticket_forms"         => "ticket_forms.json",
            "brands"               => "brands.json",
            "views"                => "views.json",
            "macros"               => "macros.json",
            "triggers"             => "triggers.json",
            "automations"          => "automations.json",
            "sla_policies"         => "slas/policies.json",
            "satisfaction_ratings" => "satisfaction_ratings.json",
            _                      => $"{resource}.json"
        };

        /// <summary>
        /// Resolves the JSON data key for the given resource.
        /// Zendesk wraps results in a key that matches the resource name.
        /// </summary>
        private static string ResolveDataKey(string resource) => resource switch
        {
            "sla_policies" => "sla_policies",
            _              => resource
        };

        // ── Timestamp conversion ─────────────────────────────────────────────

        private static long ConvertToUnixTimestamp(string dateString)
        {
            // Try parsing as ISO 8601 date/datetime
            if (DateTimeOffset.TryParse(dateString, System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AssumeUniversal, out var dto))
            {
                return dto.ToUnixTimeSeconds();
            }

            // Try parsing as a raw unix timestamp
            if (long.TryParse(dateString, out var rawUnix))
                return rawUnix;

            throw new ConnectorException(
                $"Zendesk: unable to parse start date '{dateString}' as ISO 8601 or Unix timestamp.",
                new FormatException($"Invalid date format: {dateString}"),
                "zendesk");
        }

        // ── Connection-string merging ────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON fields with explicit Parameters.
        /// ConnectionString format: {"subdomain":"...","accessToken":"...","email":"..."}
        /// </summary>
        private static void MergeConnectionString(DataSourceConfig config, out string? csSubdomain, out string? csAccessToken, out string? csEmail)
        {
            csSubdomain = null;
            csAccessToken = null;
            csEmail = null;

            if (string.IsNullOrWhiteSpace(config.ConnectionString))
                return;

            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString);
                var root = doc.RootElement;

                if (root.TryGetProperty("subdomain", out var sub) && sub.ValueKind == JsonValueKind.String)
                    csSubdomain = sub.GetString();
                if (root.TryGetProperty("accessToken", out var tok) && tok.ValueKind == JsonValueKind.String)
                    csAccessToken = tok.GetString();
                if (root.TryGetProperty("email", out var em) && em.ValueKind == JsonValueKind.String)
                    csEmail = em.GetString();
            }
            catch (JsonException)
            {
                // Not JSON — treat entire connection string as access token.
                csAccessToken = config.ConnectionString;
            }
        }

        private string ResolveSubdomain(DataSourceConfig config)
        {
            MergeConnectionString(config, out var csSubdomain, out _, out _);
            return GetStringParam(config.Parameters, "subdomain")
                ?? csSubdomain
                ?? throw new ConnectorException(
                    "Zendesk subdomain is required. Provide it via Parameters['subdomain'] or the connection string JSON.",
                    new ArgumentException("Missing 'subdomain'."),
                    "zendesk");
        }

        private string ResolveAccessToken(DataSourceConfig config)
        {
            MergeConnectionString(config, out _, out var csAccessToken, out _);
            return GetStringParam(config.Parameters, "accessToken")
                ?? csAccessToken
                ?? throw new ConnectorException(
                    "Zendesk access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "zendesk");
        }

        private string? ResolveEmail(DataSourceConfig config)
        {
            MergeConnectionString(config, out _, out _, out var csEmail);
            return GetStringParam(config.Parameters, "email") ?? csEmail;
        }

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> p, string key)
        {
            var value = GetStringParam(p, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"Zendesk connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "zendesk");
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
