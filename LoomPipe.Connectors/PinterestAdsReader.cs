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
    /// Reads advertising and content data from Pinterest using the v5 API.
    ///
    /// Parameters:
    ///   accessToken  — Pinterest OAuth bearer token
    ///   resource     — pins, boards, ad_accounts, campaigns, ad_groups, ads, analytics,
    ///                  audiences, conversions
    ///   adAccountId  — required for ad-related resources (campaigns, ad_groups, ads, analytics, audiences, conversions)
    ///   startDate    — optional ISO date for analytics (YYYY-MM-DD)
    ///   endDate      — optional ISO date for analytics (YYYY-MM-DD)
    /// </summary>
    public class PinterestAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://api.pinterest.com/v5";
        private const int DefaultPageSize = 25;

        private static readonly string[] AllResources =
        {
            "pins", "boards", "ad_accounts", "campaigns", "ad_groups",
            "ads", "analytics", "audiences", "conversions"
        };

        /// <summary>Resources that are scoped to an ad account and require adAccountId.</summary>
        private static readonly HashSet<string> AdAccountScopedResources = new(StringComparer.OrdinalIgnoreCase)
        {
            "campaigns", "ad_groups", "ads", "analytics", "audiences", "conversions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<PinterestAdsReader> _logger;

        public PinterestAdsReader(HttpClient httpClient, ILogger<PinterestAdsReader> logger)
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
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pinterest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pinterestads");
            var adAccountId = GetStringParam(config.Parameters, "adAccountId");
            var startDate   = GetStringParam(config.Parameters, "startDate");
            var endDate     = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("Pinterest: reading resource '{Resource}'.", resource);

            try
            {
                ValidateAdAccountScope(resource, adAccountId);

                List<object> records;
                if (resource == "analytics")
                {
                    records = await ReadAnalyticsAsync(accessToken, adAccountId!, startDate, endDate);
                }
                else
                {
                    records = await ReadFullAsync(resource, accessToken, adAccountId);
                }

                // Client-side watermark filtering if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records
                        .Where(r =>
                        {
                            var dict = r as IDictionary<string, object>;
                            if (dict == null || !dict.TryGetValue(watermarkField, out var val)) return false;
                            return string.Compare(val?.ToString(), watermarkValue, StringComparison.OrdinalIgnoreCase) > 0;
                        })
                        .ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pinterest: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Pinterest resource '{resource}': {ex.Message}", ex, "pinterestads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pinterest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pinterestads");
            var adAccountId = GetStringParam(config.Parameters, "adAccountId");

            _logger.LogInformation("Pinterest: discovering schema for '{Resource}'.", resource);

            try
            {
                ValidateAdAccountScope(resource, adAccountId);

                List<object> sample;
                if (resource == "analytics")
                {
                    var startDate = GetStringParam(config.Parameters, "startDate");
                    var endDate   = GetStringParam(config.Parameters, "endDate");
                    sample = await ReadAnalyticsAsync(accessToken, adAccountId!, startDate, endDate);
                }
                else
                {
                    sample = await ReadFullAsync(resource, accessToken, adAccountId, maxPages: 1);
                }

                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pinterest: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Pinterest schema for '{resource}': {ex.Message}", ex, "pinterestads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pinterest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pinterestads");
            var adAccountId = GetStringParam(config.Parameters, "adAccountId");

            _logger.LogInformation("Pinterest: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                ValidateAdAccountScope(resource, adAccountId);

                List<object> records;
                if (resource == "analytics")
                {
                    var startDate = GetStringParam(config.Parameters, "startDate");
                    var endDate   = GetStringParam(config.Parameters, "endDate");
                    records = await ReadAnalyticsAsync(accessToken, adAccountId!, startDate, endDate);
                }
                else
                {
                    records = await ReadFullAsync(resource, accessToken, adAccountId, maxPages: 1);
                }

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pinterest: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Pinterest dry run preview failed for '{resource}': {ex.Message}", ex, "pinterestads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated with bookmark token) ────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? adAccountId,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? bookmark = null;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, adAccountId, bookmark);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);

                int added = results.Count - countBefore;
                if (added == 0) break;

                // Pinterest uses a "bookmark" token for cursor-based pagination.
                bookmark = null;
                if (doc.RootElement.TryGetProperty("bookmark", out var bookmarkProp)
                    && bookmarkProp.ValueKind == JsonValueKind.String)
                {
                    bookmark = bookmarkProp.GetString();
                }

                page++;
            }
            while (!string.IsNullOrEmpty(bookmark) && page < maxPages);

            _logger.LogInformation("Pinterest: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page + 1);

            return results;
        }

        // ── Analytics read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadAnalyticsAsync(
            string accessToken, string adAccountId, string? startDate, string? endDate)
        {
            var results = new List<object>();

            // Default to last 30 days if no dates provided.
            var end   = endDate   ?? DateTime.UtcNow.ToString("yyyy-MM-dd");
            var start = startDate ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");

            var escapedAdAccountId = Uri.EscapeDataString(adAccountId);
            var url = $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/analytics"
                + $"?start_date={Uri.EscapeDataString(start)}"
                + $"&end_date={Uri.EscapeDataString(end)}"
                + "&granularity=DAY";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            ParseResultsPage(doc.RootElement, results);

            _logger.LogInformation("Pinterest: analytics returned {Count} records.", results.Count);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? adAccountId, string? bookmark)
        {
            string endpoint;

            if (AdAccountScopedResources.Contains(resource))
            {
                var escapedAdAccountId = Uri.EscapeDataString(adAccountId!);
                endpoint = resource switch
                {
                    "campaigns"   => $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/campaigns",
                    "ad_groups"   => $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/ad_groups",
                    "ads"         => $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/ads",
                    "audiences"   => $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/audiences",
                    "conversions" => $"{BaseUrl}/ad_accounts/{escapedAdAccountId}/conversion_tags",
                    _ => throw new ConnectorException(
                        $"Unknown Pinterest resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "pinterestads")
                };
            }
            else
            {
                endpoint = resource switch
                {
                    "pins"        => $"{BaseUrl}/pins",
                    "boards"      => $"{BaseUrl}/boards",
                    "ad_accounts" => $"{BaseUrl}/ad_accounts",
                    _ => throw new ConnectorException(
                        $"Unknown Pinterest resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "pinterestads")
                };
            }

            var sb = new StringBuilder(endpoint);
            sb.Append($"?page_size={DefaultPageSize}");

            if (!string.IsNullOrEmpty(bookmark))
            {
                sb.Append($"&bookmark={Uri.EscapeDataString(bookmark)}");
            }

            return sb.ToString();
        }

        // ── Validation ───────────────────────────────────────────────────────

        private static void ValidateAdAccountScope(string resource, string? adAccountId)
        {
            if (AdAccountScopedResources.Contains(resource) && string.IsNullOrWhiteSpace(adAccountId))
            {
                throw new ConnectorException(
                    $"Pinterest resource '{resource}' requires the 'adAccountId' parameter.",
                    new ArgumentException($"Missing 'adAccountId' for ad-account-scoped resource: {resource}"),
                    "pinterestads");
            }
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            // Pinterest wraps paginated results in an "items" array.
            if (root.TryGetProperty("items", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard paginated response
            }
            else if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Alternative wrapper
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                // Single object response
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(root, row);
                if (row.Count > 0)
                    results.Add(row);
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(element, row);
                results.Add(row);
            }
        }

        private static void FlattenJsonObject(JsonElement element, IDictionary<string, object> row)
        {
            if (element.ValueKind != JsonValueKind.Object) return;

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
            }
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String  => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number  => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True    => true,
                JsonValueKind.False   => false,
                JsonValueKind.Null    => string.Empty,
                _                     => value.ToString()
            };
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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
                    $"Pinterest connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "pinterestads");
            return value;
        }
    }
}
