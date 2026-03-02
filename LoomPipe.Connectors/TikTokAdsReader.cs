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
    /// Reads advertising data from TikTok using the Marketing API v1.3.
    ///
    /// Parameters:
    ///   accessToken   — TikTok Marketing API access token
    ///   advertiserId  — TikTok advertiser ID
    ///   resource      — campaigns, ad_groups, ads, reports, audiences, creatives, pixels
    ///   startDate     — optional ISO date for report filtering (YYYY-MM-DD)
    ///   endDate       — optional ISO date for report filtering (YYYY-MM-DD)
    /// </summary>
    public class TikTokAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://business-api.tiktok.com/open_api/v1.3";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "campaigns", "ad_groups", "ads", "reports", "audiences", "creatives", "pixels"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TikTokAdsReader> _logger;

        public TikTokAdsReader(HttpClient httpClient, ILogger<TikTokAdsReader> logger)
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
            var resource     = GetRequiredParam(config.Parameters, "resource");
            var accessToken  = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "TikTok Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "tiktokads");
            var advertiserId = GetRequiredParam(config.Parameters, "advertiserId");
            var startDate    = GetStringParam(config.Parameters, "startDate");
            var endDate      = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("TikTokAds: reading resource '{Resource}'.", resource);

            try
            {
                if (resource == "reports")
                {
                    return await ReadReportsAsync(accessToken, advertiserId, startDate, endDate);
                }

                return await ReadFullAsync(resource, accessToken, advertiserId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TikTokAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read TikTok Ads resource '{resource}': {ex.Message}", ex, "tiktokads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource     = GetRequiredParam(config.Parameters, "resource");
            var accessToken  = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "TikTok Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "tiktokads");
            var advertiserId = GetRequiredParam(config.Parameters, "advertiserId");

            _logger.LogInformation("TikTokAds: discovering schema for '{Resource}'.", resource);

            try
            {
                List<object> sample;
                if (resource == "reports")
                {
                    // Use a recent 7-day window for schema discovery.
                    var end = DateTime.UtcNow.ToString("yyyy-MM-dd");
                    var start = DateTime.UtcNow.AddDays(-7).ToString("yyyy-MM-dd");
                    sample = await ReadReportsAsync(accessToken, advertiserId, start, end, maxPages: 1);
                }
                else
                {
                    sample = await ReadFullAsync(resource, accessToken, advertiserId, maxPages: 1);
                }

                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TikTokAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover TikTok Ads schema for '{resource}': {ex.Message}", ex, "tiktokads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource     = GetRequiredParam(config.Parameters, "resource");
            var accessToken  = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "TikTok Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "tiktokads");
            var advertiserId = GetRequiredParam(config.Parameters, "advertiserId");

            _logger.LogInformation("TikTokAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                List<object> records;
                if (resource == "reports")
                {
                    var end = DateTime.UtcNow.ToString("yyyy-MM-dd");
                    var start = DateTime.UtcNow.AddDays(-7).ToString("yyyy-MM-dd");
                    records = await ReadReportsAsync(accessToken, advertiserId, start, end, maxPages: 1);
                }
                else
                {
                    records = await ReadFullAsync(resource, accessToken, advertiserId, maxPages: 1);
                }

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TikTokAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"TikTok Ads dry run preview failed for '{resource}': {ex.Message}", ex, "tiktokads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string advertiserId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;

            var endpoint = resource switch
            {
                "campaigns"  => "/campaign/get/",
                "ad_groups"  => "/adgroup/get/",
                "ads"        => "/ad/get/",
                "audiences"  => "/dmp/custom_audience/list/",
                "creatives"  => "/creative/get/",
                "pixels"     => "/pixel/list/",
                _            => throw new ConnectorException(
                    $"Unsupported TikTok Ads resource: '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "tiktokads")
            };

            do
            {
                var url = $"{BaseUrl}{endpoint}?advertiser_id={Uri.EscapeDataString(advertiserId)}&page_size={PageSize}&page={page}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int totalNumber = 0;
                if (doc.RootElement.TryGetProperty("data", out var data))
                {
                    if (data.TryGetProperty("page_info", out var pageInfo)
                        && pageInfo.TryGetProperty("total_number", out var totalEl))
                    {
                        totalNumber = totalEl.GetInt32();
                    }

                    if (data.TryGetProperty("list", out var list) && list.ValueKind == JsonValueKind.Array)
                    {
                        FlattenJsonArray(list, results);
                    }
                }

                page++;

                if (results.Count >= totalNumber || totalNumber == 0)
                    break;
            }
            while (page <= maxPages);

            _logger.LogInformation("TikTokAds: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page - 1);

            return results;
        }

        // ── Reports read (POST) ──────────────────────────────────────────────

        private async Task<List<object>> ReadReportsAsync(
            string accessToken, string advertiserId,
            string? startDate, string? endDate, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;

            var start = startDate ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
            var end = endDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd");

            do
            {
                var bodyObj = new
                {
                    advertiser_id = advertiserId,
                    report_type = "BASIC",
                    dimensions = new[] { "campaign_id" },
                    data_level = "AUCTION_CAMPAIGN",
                    metrics = new[] { "spend", "impressions", "clicks", "ctr", "cpc", "conversions" },
                    start_date = start,
                    end_date = end,
                    page,
                    page_size = PageSize
                };

                var bodyJson = JsonSerializer.Serialize(bodyObj);

                using var request = new HttpRequestMessage(HttpMethod.Post, $"{BaseUrl}/report/integrated/get/")
                {
                    Content = new StringContent(bodyJson, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int totalNumber = 0;
                if (doc.RootElement.TryGetProperty("data", out var data))
                {
                    if (data.TryGetProperty("page_info", out var pageInfo)
                        && pageInfo.TryGetProperty("total_number", out var totalEl))
                    {
                        totalNumber = totalEl.GetInt32();
                    }

                    if (data.TryGetProperty("list", out var list) && list.ValueKind == JsonValueKind.Array)
                    {
                        FlattenJsonArray(list, results);
                    }
                }

                page++;

                if (results.Count >= totalNumber || totalNumber == 0)
                    break;
            }
            while (page <= maxPages);

            _logger.LogInformation("TikTokAds: read {Count} report records across {Pages} page(s).",
                results.Count, page - 1);

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
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten nested objects (e.g. "metrics", "dimensions").
                        foreach (var inner in prop.Value.EnumerateObject())
                        {
                            row[inner.Name] = ConvertJsonValue(inner.Value);
                        }
                    }
                    else
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
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
            // TikTok Marketing API uses the Access-Token header (not Bearer).
            request.Headers.Add("Access-Token", accessToken);
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
                    $"TikTok Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "tiktokads");
            return value;
        }
    }
}
