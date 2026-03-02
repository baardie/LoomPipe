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
    /// Reads search analytics and site data from Google Search Console using the v3 API.
    ///
    /// Parameters:
    ///   accessToken — Google OAuth2 access token
    ///   siteUrl     — URL-encoded Search Console property (e.g. "https://example.com" or "sc-domain:example.com")
    ///   resource    — performance, sitemaps, sites
    ///   startDate   — ISO date (YYYY-MM-DD) for performance queries
    ///   endDate     — ISO date (YYYY-MM-DD) for performance queries
    ///   dimensions  — optional comma-separated dimensions: query, page, country, device, date
    /// </summary>
    public class GoogleSearchConsoleReader : ISourceReader
    {
        private const string BaseUrl = "https://searchconsole.googleapis.com/webmasters/v3";
        private const int RowLimit = 25000;

        private static readonly string[] AllResources =
        {
            "performance", "sitemaps", "sites"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleSearchConsoleReader> _logger;

        public GoogleSearchConsoleReader(HttpClient httpClient, ILogger<GoogleSearchConsoleReader> logger)
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
                    "Google Search Console access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googlesearchconsole");
            var siteUrl    = GetRequiredParam(config.Parameters, "siteUrl");
            var startDate  = GetStringParam(config.Parameters, "startDate");
            var endDate    = GetStringParam(config.Parameters, "endDate");
            var dimensions = GetStringParam(config.Parameters, "dimensions");

            _logger.LogInformation("GoogleSearchConsole: reading resource '{Resource}'.", resource);

            try
            {
                return resource switch
                {
                    "performance" => await ReadPerformanceAsync(accessToken, siteUrl, startDate, endDate, dimensions),
                    "sitemaps"    => await ReadSitemapsAsync(accessToken, siteUrl),
                    "sites"       => await ReadSitesAsync(accessToken),
                    _             => throw new ConnectorException(
                        $"Unsupported Google Search Console resource: '{resource}'.",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "googlesearchconsole")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleSearchConsole: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Google Search Console resource '{resource}': {ex.Message}", ex, "googlesearchconsole");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Google Search Console access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googlesearchconsole");

            _logger.LogInformation("GoogleSearchConsole: discovering schema for '{Resource}'.", resource);

            try
            {
                // For performance, return the known schema fields.
                if (resource == "performance")
                {
                    var dimensions = GetStringParam(config.Parameters, "dimensions") ?? "query,page,country,device";
                    var fields = new List<string>();
                    foreach (var dim in dimensions.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    {
                        fields.Add(dim);
                    }
                    fields.AddRange(new[] { "clicks", "impressions", "ctr", "position" });
                    return fields;
                }

                // For other resources, fetch a sample and inspect the keys.
                var siteUrl = GetRequiredParam(config.Parameters, "siteUrl");
                List<object> sample = resource switch
                {
                    "sitemaps" => await ReadSitemapsAsync(accessToken, siteUrl),
                    "sites"    => await ReadSitesAsync(accessToken),
                    _          => new List<object>()
                };

                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleSearchConsole: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Google Search Console schema for '{resource}': {ex.Message}", ex, "googlesearchconsole");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Google Search Console access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googlesearchconsole");

            _logger.LogInformation("GoogleSearchConsole: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var siteUrl    = GetRequiredParam(config.Parameters, "siteUrl");
                var startDate  = GetStringParam(config.Parameters, "startDate");
                var endDate    = GetStringParam(config.Parameters, "endDate");
                var dimensions = GetStringParam(config.Parameters, "dimensions");

                List<object> records = resource switch
                {
                    "performance" => await ReadPerformanceAsync(accessToken, siteUrl, startDate, endDate, dimensions, maxRows: sampleSize),
                    "sitemaps"    => await ReadSitemapsAsync(accessToken, siteUrl),
                    "sites"       => await ReadSitesAsync(accessToken),
                    _             => new List<object>()
                };

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleSearchConsole: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Google Search Console dry run preview failed for '{resource}': {ex.Message}", ex, "googlesearchconsole");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Performance (search analytics) ───────────────────────────────────

        private async Task<List<object>> ReadPerformanceAsync(
            string accessToken, string siteUrl,
            string? startDate, string? endDate, string? dimensions,
            int maxRows = int.MaxValue)
        {
            var results = new List<object>();
            int startRow = 0;

            var start = startDate ?? DateTime.UtcNow.AddDays(-28).ToString("yyyy-MM-dd");
            var end = endDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd");

            var dimList = (dimensions ?? "query,page,country,device")
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            var encodedSiteUrl = Uri.EscapeDataString(siteUrl);

            do
            {
                var bodyObj = new Dictionary<string, object>
                {
                    ["startDate"] = start,
                    ["endDate"] = end,
                    ["dimensions"] = dimList,
                    ["rowLimit"] = Math.Min(RowLimit, maxRows - results.Count),
                    ["startRow"] = startRow
                };

                var bodyJson = JsonSerializer.Serialize(bodyObj);

                using var request = new HttpRequestMessage(HttpMethod.Post,
                    $"{BaseUrl}/sites/{encodedSiteUrl}/searchAnalytics/query")
                {
                    Content = new StringContent(bodyJson, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int rowsInPage = 0;
                if (doc.RootElement.TryGetProperty("rows", out var rows) && rows.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in rows.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        // Map keys array to dimension names.
                        if (element.TryGetProperty("keys", out var keys) && keys.ValueKind == JsonValueKind.Array)
                        {
                            int i = 0;
                            foreach (var key in keys.EnumerateArray())
                            {
                                var dimName = i < dimList.Length ? dimList[i] : $"key_{i}";
                                row[dimName] = key.GetString() ?? string.Empty;
                                i++;
                            }
                        }

                        if (element.TryGetProperty("clicks", out var clicks))
                            row["clicks"] = ConvertJsonValue(clicks);
                        if (element.TryGetProperty("impressions", out var impressions))
                            row["impressions"] = ConvertJsonValue(impressions);
                        if (element.TryGetProperty("ctr", out var ctr))
                            row["ctr"] = ConvertJsonValue(ctr);
                        if (element.TryGetProperty("position", out var position))
                            row["position"] = ConvertJsonValue(position);

                        results.Add(row);
                        rowsInPage++;
                    }
                }

                startRow += rowsInPage;

                // No more rows to fetch.
                if (rowsInPage < RowLimit || results.Count >= maxRows)
                    break;
            }
            while (true);

            _logger.LogInformation("GoogleSearchConsole: read {Count} performance rows.", results.Count);

            return results;
        }

        // ── Sitemaps ─────────────────────────────────────────────────────────

        private async Task<List<object>> ReadSitemapsAsync(string accessToken, string siteUrl)
        {
            var results = new List<object>();
            var encodedSiteUrl = Uri.EscapeDataString(siteUrl);

            using var request = new HttpRequestMessage(HttpMethod.Get,
                $"{BaseUrl}/sites/{encodedSiteUrl}/sitemaps");
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("sitemap", out var sitemaps) && sitemaps.ValueKind == JsonValueKind.Array)
            {
                FlattenJsonArray(sitemaps, results);
            }

            _logger.LogInformation("GoogleSearchConsole: read {Count} sitemaps.", results.Count);

            return results;
        }

        // ── Sites ────────────────────────────────────────────────────────────

        private async Task<List<object>> ReadSitesAsync(string accessToken)
        {
            var results = new List<object>();

            using var request = new HttpRequestMessage(HttpMethod.Get, $"{BaseUrl}/sites");
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("siteEntry", out var sites) && sites.ValueKind == JsonValueKind.Array)
            {
                FlattenJsonArray(sites, results);
            }

            _logger.LogInformation("GoogleSearchConsole: read {Count} sites.", results.Count);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void FlattenJsonArray(JsonElement array, List<object> results)
        {
            foreach (var element in array.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                results.Add(row);
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
                    $"Google Search Console connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "googlesearchconsole");
            return value;
        }
    }
}
