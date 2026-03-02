#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads advertising data from Meta (Facebook) using the Marketing API v19.0.
    ///
    /// Parameters:
    ///   accessToken   — Meta Marketing API access token
    ///   adAccountId   — Ad account ID in the format act_XXXXXXX
    ///   resource      — campaigns, adsets, ads, insights, adcreatives, audiences, adimages
    ///   dateRange     — optional date range for insights (e.g. "last_30d", "last_7d", "last_90d")
    ///   level         — optional insights breakdown level: account, campaign, adset, ad
    ///   startDate     — optional ISO date for time_range "since" (e.g. "2024-01-01")
    ///   endDate       — optional ISO date for time_range "until" (e.g. "2024-12-31")
    /// </summary>
    public class FacebookAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://graph.facebook.com/v19.0";
        private const int DefaultLimit = 100;
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "campaigns", "adsets", "ads", "insights",
            "adcreatives", "audiences", "adimages"
        };

        /// <summary>
        /// Default fields to request per resource.
        /// </summary>
        private static readonly Dictionary<string, string> DefaultFields = new(StringComparer.OrdinalIgnoreCase)
        {
            ["campaigns"]   = "id,name,status,objective,created_time,updated_time,start_time,stop_time,daily_budget,lifetime_budget",
            ["adsets"]      = "id,name,status,campaign_id,daily_budget,lifetime_budget,targeting,start_time,end_time,optimization_goal",
            ["ads"]         = "id,name,status,adset_id,campaign_id,creative,created_time,updated_time",
            ["insights"]    = "impressions,clicks,spend,cpc,cpm,ctr,reach,frequency,actions,cost_per_action_type",
            ["adcreatives"] = "id,name,title,body,image_url,thumbnail_url,object_story_spec,status",
            ["audiences"]   = "id,name,subtype,approximate_count,description",
            ["adimages"]    = "hash,name,url,width,height,created_time"
        };

        /// <summary>
        /// Maps date range shorthand to number of days to look back.
        /// </summary>
        private static readonly Dictionary<string, int> DateRangeLookback = new(StringComparer.OrdinalIgnoreCase)
        {
            ["last_7d"]  = 7,
            ["last_14d"] = 14,
            ["last_30d"] = 30,
            ["last_60d"] = 60,
            ["last_90d"] = 90
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<FacebookAdsReader> _logger;

        public FacebookAdsReader(HttpClient httpClient, ILogger<FacebookAdsReader> logger)
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
            MergeConnectionString(config);

            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetRequiredParam(config.Parameters, "accessToken");
            var adAccountId = GetRequiredParam(config.Parameters, "adAccountId");
            var dateRange   = GetStringParam(config.Parameters, "dateRange");
            var level       = GetStringParam(config.Parameters, "level");
            var startDate   = GetStringParam(config.Parameters, "startDate");
            var endDate     = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("FacebookAds: reading resource '{Resource}' for account {AccountId}.", resource, adAccountId);

            try
            {
                var url = BuildUrl(adAccountId, resource, accessToken, dateRange, level, startDate, endDate);

                // Append watermark filter if provided
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    url += $"&filtering=[{{\"field\":\"{watermarkField}\",\"operator\":\"GREATER_THAN\",\"value\":\"{watermarkValue}\"}}]";
                }

                return await ReadPaginatedAsync(url, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FacebookAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Facebook Ads resource '{resource}': {ex.Message}", ex, "facebookads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("FacebookAds: discovering schema for '{Resource}'.", resource);

            try
            {
                // Fetch a small sample and return the keys of the first record
                var records = await ReadAsync(config);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FacebookAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Facebook Ads schema for '{resource}': {ex.Message}", ex, "facebookads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("FacebookAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FacebookAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"FacebookAds dry run preview failed for '{resource}': {ex.Message}", ex, "facebookads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(
            string adAccountId, string resource, string accessToken,
            string? dateRange, string? level, string? startDate, string? endDate)
        {
            var fields = DefaultFields.TryGetValue(resource, out var defaultFields)
                ? defaultFields
                : "id,name";

            var url = $"{BaseUrl}/{adAccountId}/{resource}?fields={Uri.EscapeDataString(fields)}&limit={DefaultLimit}" +
                      $"&access_token={Uri.EscapeDataString(accessToken)}";

            // Insights-specific parameters
            if (resource.Equals("insights", StringComparison.OrdinalIgnoreCase))
            {
                var timeRange = BuildTimeRange(dateRange, startDate, endDate);
                if (!string.IsNullOrEmpty(timeRange))
                {
                    url += $"&time_range={Uri.EscapeDataString(timeRange)}";
                }

                if (!string.IsNullOrEmpty(level))
                {
                    url += $"&level={Uri.EscapeDataString(level)}";
                }
            }

            return url;
        }

        private static string? BuildTimeRange(string? dateRange, string? startDate, string? endDate)
        {
            // Explicit start/end dates take precedence
            if (!string.IsNullOrEmpty(startDate))
            {
                var until = !string.IsNullOrEmpty(endDate)
                    ? endDate
                    : DateTime.UtcNow.ToString("yyyy-MM-dd");

                return $"{{\"since\":\"{startDate}\",\"until\":\"{until}\"}}";
            }

            // Shorthand date range
            if (!string.IsNullOrEmpty(dateRange) && DateRangeLookback.TryGetValue(dateRange, out var days))
            {
                var since = DateTime.UtcNow.AddDays(-days).ToString("yyyy-MM-dd");
                var until = DateTime.UtcNow.ToString("yyyy-MM-dd");
                return $"{{\"since\":\"{since}\",\"until\":\"{until}\"}}";
            }

            return null;
        }

        // ── Paginated read ──────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(string initialUrl, string accessToken, string resource)
        {
            var results = new List<object>();
            string? nextUrl = initialUrl;

            while (!string.IsNullOrEmpty(nextUrl))
            {
                var json = await SendWithRetryAsync(nextUrl, accessToken);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse data array
                if (root.TryGetProperty("data", out var dataArray)
                    && dataArray.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in dataArray.EnumerateArray())
                    {
                        results.Add(ParseFacebookObject(element));
                    }
                }

                // Check for next page via paging.next
                nextUrl = null;
                if (root.TryGetProperty("paging", out var paging)
                    && paging.TryGetProperty("next", out var next)
                    && next.ValueKind == JsonValueKind.String)
                {
                    nextUrl = next.GetString();
                }
            }

            _logger.LogInformation("FacebookAds: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── HTTP with retry / rate-limit handling ────────────────────────────

        private async Task<string> SendWithRetryAsync(string url, string accessToken)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("FacebookAds rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Facebook Ads API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "facebookads");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("FacebookAds API error {StatusCode}: {Body}",
                        (int)response.StatusCode, errorBody);

                    throw new ConnectorException(
                        $"Facebook Ads API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "facebookads");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "Facebook Ads request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "facebookads");
        }

        // ── JSON parsing ─────────────────────────────────────────────────────

        private static IDictionary<string, object> ParseFacebookObject(JsonElement element)
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
                    // Nested objects and arrays are serialized as JSON strings
                    JsonValueKind.Object => prop.Value.GetRawText(),
                    JsonValueKind.Array  => prop.Value.GetRawText(),
                    _                    => prop.Value.ToString()
                };
            }

            return expando;
        }

        // ── Connection-string merge ─────────────────────────────────────────

        private static void MergeConnectionString(DataSourceConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString))
                return;

            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    return;

                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!config.Parameters.ContainsKey(prop.Name))
                    {
                        config.Parameters[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                            ? prop.Value.GetString()!
                            : prop.Value.GetRawText();
                    }
                }
            }
            catch (JsonException)
            {
                // Not JSON — treat as a plain access token
                if (!config.Parameters.ContainsKey("accessToken"))
                    config.Parameters["accessToken"] = config.ConnectionString;
            }
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
                    $"Facebook Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "facebookads");
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
