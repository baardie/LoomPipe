#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
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
    /// Reads advertising data from Snapchat using the Marketing API (v1).
    ///
    /// Parameters:
    ///   accessToken    — Snapchat OAuth access token
    ///   resource       — organizations, ad_accounts, campaigns, ad_squads, ads,
    ///                    creatives, media, targeting, stats
    ///   adAccountId    — Snapchat Ad Account ID (required for most resources)
    ///   organizationId — Snapchat Organization ID (required for ad_accounts)
    /// </summary>
    public class SnapchatAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://adsapi.snapchat.com/v1";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "organizations", "ad_accounts", "campaigns", "ad_squads", "ads",
            "creatives", "media", "targeting", "stats"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SnapchatAdsReader> _logger;

        public SnapchatAdsReader(HttpClient httpClient, ILogger<SnapchatAdsReader> logger)
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
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SnapchatAds: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(parameters, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SnapchatAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Snapchat Ads resource '{resource}': {ex.Message}", ex, "snapchatads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SnapchatAds: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(parameters, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SnapchatAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Snapchat Ads schema for '{resource}': {ex.Message}", ex, "snapchatads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SnapchatAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(parameters, resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SnapchatAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Snapchat Ads dry run preview failed for '{resource}': {ex.Message}", ex, "snapchatads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            Dictionary<string, object> parameters, string resource,
            string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var adAccountId    = GetStringParam(parameters, "adAccountId");
            var organizationId = GetStringParam(parameters, "organizationId");
            string? nextLink   = null;
            int page = 0;

            do
            {
                var url = nextLink ?? BuildUrl(resource, adAccountId, organizationId);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, resource, results);

                // Pagination via paging.next_link
                nextLink = null;
                if (doc.RootElement.TryGetProperty("paging", out var paging)
                    && paging.TryGetProperty("next_link", out var nextLinkEl)
                    && nextLinkEl.ValueKind == JsonValueKind.String)
                {
                    nextLink = nextLinkEl.GetString();
                }

                page++;
            }
            while (nextLink != null && page < maxPages);

            _logger.LogInformation("SnapchatAds: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? adAccountId, string? organizationId)
        {
            return resource switch
            {
                "organizations" => $"{BaseUrl}/me/organizations",
                "ad_accounts"   => $"{BaseUrl}/organizations/{organizationId}/adaccounts?limit={PageLimit}",
                "campaigns"     => $"{BaseUrl}/adaccounts/{adAccountId}/campaigns?limit={PageLimit}",
                "ad_squads"     => $"{BaseUrl}/adaccounts/{adAccountId}/adsquads?limit={PageLimit}",
                "ads"           => $"{BaseUrl}/adaccounts/{adAccountId}/ads?limit={PageLimit}",
                "creatives"     => $"{BaseUrl}/adaccounts/{adAccountId}/creatives?limit={PageLimit}",
                "media"         => $"{BaseUrl}/adaccounts/{adAccountId}/media?limit={PageLimit}",
                "targeting"     => $"{BaseUrl}/adaccounts/{adAccountId}/targeting?limit={PageLimit}",
                "stats"         => BuildStatsUrl(adAccountId),
                _               => throw new ConnectorException(
                    $"Unsupported Snapchat Ads resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "snapchatads")
            };
        }

        private static string BuildStatsUrl(string? adAccountId)
        {
            var end   = DateTime.UtcNow;
            var start = end.AddDays(-30);
            var startStr = start.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'-0000'");
            var endStr   = end.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'-0000'");
            return $"{BaseUrl}/adaccounts/{adAccountId}/stats?granularity=DAY&start_time={Uri.EscapeDataString(startStr)}&end_time={Uri.EscapeDataString(endStr)}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            // Snapchat wraps results in a plural resource key, e.g. "campaigns" → [{campaign: {...}}]
            var collectionKey = GetCollectionKey(resource);

            JsonElement items;
            if (root.TryGetProperty(collectionKey, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Each element may be wrapped: { "campaign": { ... } }
                var singularKey = GetSingularKey(resource);
                foreach (var wrapper in items.EnumerateArray())
                {
                    JsonElement element;
                    if (wrapper.TryGetProperty(singularKey, out element))
                    {
                        results.Add(FlattenElement(element));
                    }
                    else
                    {
                        results.Add(FlattenElement(wrapper));
                    }
                }
            }
            else if (root.TryGetProperty("timeseries_stats", out var stats) && stats.ValueKind == JsonValueKind.Array)
            {
                // Stats response
                foreach (var stat in stats.EnumerateArray())
                {
                    results.Add(FlattenElement(stat));
                }
            }
            else if (root.TryGetProperty("request_status", out _))
            {
                // Single-item response or empty
                if (root.TryGetProperty(GetSingularKey(resource), out var single))
                {
                    results.Add(FlattenElement(single));
                }
            }
        }

        private static string GetCollectionKey(string resource) => resource switch
        {
            "organizations" => "organizations",
            "ad_accounts"   => "adaccounts",
            "campaigns"     => "campaigns",
            "ad_squads"     => "adsquads",
            "ads"           => "ads",
            "creatives"     => "creatives",
            "media"         => "media",
            "targeting"     => "targeting",
            "stats"         => "timeseries_stats",
            _               => resource
        };

        private static string GetSingularKey(string resource) => resource switch
        {
            "organizations" => "organization",
            "ad_accounts"   => "adaccount",
            "campaigns"     => "campaign",
            "ad_squads"     => "adsquad",
            "ads"           => "ad",
            "creatives"     => "creative",
            "media"         => "media",
            "targeting"     => "targeting",
            "stats"         => "timeseries_stat",
            _               => resource
        };

        private static object FlattenElement(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
            }

            return row;
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True   => true,
                JsonValueKind.False  => false,
                JsonValueKind.Null   => string.Empty,
                _                    => value.ToString()
            };
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"accessToken":"...","adAccountId":"...","organizationId":"..."}
        /// Parameters take precedence; connection string provides defaults.
        /// </summary>
        private static Dictionary<string, object> MergeConnectionString(DataSourceConfig config)
        {
            var merged = new Dictionary<string, object>(config.Parameters ?? new Dictionary<string, object>());
            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString ?? "{}");
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!merged.ContainsKey(prop.Name) || string.IsNullOrWhiteSpace(GetStringParam(merged, prop.Name)))
                        merged[prop.Name] = prop.Value.Clone();
                }
            }
            catch (JsonException) { /* not JSON — ignore */ }
            return merged;
        }

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (string.IsNullOrWhiteSpace(token))
                throw new ConnectorException(
                    "Snapchat Ads access token is required. Provide it via Parameters['accessToken'] or the connection string JSON.",
                    new ArgumentException("Missing 'accessToken'."),
                    "snapchatads");
            return token;
        }

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> parameters, string key)
        {
            var value = GetStringParam(parameters, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"Snapchat Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "snapchatads");
            return value;
        }
    }
}
