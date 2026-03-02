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
    /// Reads advertising data from LinkedIn using the Marketing REST API.
    ///
    /// Parameters:
    ///   accessToken   — LinkedIn OAuth 2.0 access token
    ///   resource      — adAccounts, campaigns, creatives, analytics, conversions
    ///   adAccountId   — LinkedIn ad account ID (required for campaigns, creatives, analytics)
    ///   startDate     — optional ISO date for analytics date range start (e.g. "2024-01-01")
    ///   endDate       — optional ISO date for analytics date range end (e.g. "2024-12-31")
    ///   pivot         — optional analytics pivot: COMPANY, CAMPAIGN, CREATIVE, etc.
    ///   timeGranularity — optional: DAILY, MONTHLY, ALL (default ALL)
    /// </summary>
    public class LinkedInAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://api.linkedin.com/rest";
        private const int DefaultCount = 100;
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "adAccounts", "campaigns", "creatives", "analytics", "conversions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<LinkedInAdsReader> _logger;

        public LinkedInAdsReader(HttpClient httpClient, ILogger<LinkedInAdsReader> logger)
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
            var adAccountId = GetStringParam(config.Parameters, "adAccountId");
            var startDate   = GetStringParam(config.Parameters, "startDate");
            var endDate     = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("LinkedInAds: reading resource '{Resource}'.", resource);

            try
            {
                return resource.ToLowerInvariant() switch
                {
                    "adaccounts" => await ReadAdAccountsAsync(accessToken),
                    "campaigns"  => await ReadCampaignsAsync(accessToken, RequireAdAccountId(adAccountId)),
                    "creatives"  => await ReadCreativesAsync(accessToken, RequireAdAccountId(adAccountId)),
                    "analytics"  => await ReadAnalyticsAsync(accessToken, RequireAdAccountId(adAccountId), startDate, endDate, config.Parameters),
                    "conversions" => await ReadConversionsAsync(accessToken, RequireAdAccountId(adAccountId)),
                    _ => throw new ConnectorException(
                        $"Unknown LinkedIn Ads resource '{resource}'. Valid resources: {string.Join(", ", AllResources)}.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "linkedinads")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "LinkedInAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read LinkedIn Ads resource '{resource}': {ex.Message}", ex, "linkedinads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("LinkedInAds: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadAsync(config);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "LinkedInAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover LinkedIn Ads schema for '{resource}': {ex.Message}", ex, "linkedinads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("LinkedInAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "LinkedInAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"LinkedInAds dry run preview failed for '{resource}': {ex.Message}", ex, "linkedinads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Resource-specific readers ───────────────────────────────────────

        private async Task<List<object>> ReadAdAccountsAsync(string accessToken)
        {
            var url = $"{BaseUrl}/adAccounts?q=search&count={DefaultCount}";
            return await ReadPaginatedAsync(url, accessToken, "adAccounts");
        }

        private async Task<List<object>> ReadCampaignsAsync(string accessToken, string adAccountId)
        {
            var url = $"{BaseUrl}/adAccounts/{adAccountId}/campaigns?count={DefaultCount}";
            return await ReadPaginatedAsync(url, accessToken, "campaigns");
        }

        private async Task<List<object>> ReadCreativesAsync(string accessToken, string adAccountId)
        {
            var url = $"{BaseUrl}/adAccounts/{adAccountId}/creatives?count={DefaultCount}";
            return await ReadPaginatedAsync(url, accessToken, "creatives");
        }

        private async Task<List<object>> ReadAnalyticsAsync(
            string accessToken, string adAccountId,
            string? startDate, string? endDate,
            Dictionary<string, object> parameters)
        {
            var pivot = GetStringParam(parameters, "pivot") ?? "CAMPAIGN";
            var granularity = GetStringParam(parameters, "timeGranularity") ?? "ALL";

            // Default date range: last 30 days
            var since = !string.IsNullOrEmpty(startDate) ? startDate : DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
            var until = !string.IsNullOrEmpty(endDate) ? endDate : DateTime.UtcNow.ToString("yyyy-MM-dd");

            // Parse dates for the LinkedIn date format
            var sinceDate = DateTime.Parse(since);
            var untilDate = DateTime.Parse(until);

            var url = $"{BaseUrl}/adAnalytics?q=analytics" +
                      $"&pivot={Uri.EscapeDataString(pivot)}" +
                      $"&timeGranularity={Uri.EscapeDataString(granularity)}" +
                      $"&dateRange.start.year={sinceDate.Year}&dateRange.start.month={sinceDate.Month}&dateRange.start.day={sinceDate.Day}" +
                      $"&dateRange.end.year={untilDate.Year}&dateRange.end.month={untilDate.Month}&dateRange.end.day={untilDate.Day}" +
                      $"&accounts=urn:li:sponsoredAccount:{adAccountId}" +
                      $"&count={DefaultCount}";

            return await ReadPaginatedAsync(url, accessToken, "analytics");
        }

        private async Task<List<object>> ReadConversionsAsync(string accessToken, string adAccountId)
        {
            var url = $"{BaseUrl}/adAccounts/{adAccountId}/conversions?count={DefaultCount}";
            return await ReadPaginatedAsync(url, accessToken, "conversions");
        }

        // ── Paginated read ──────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(string initialUrl, string accessToken, string resource)
        {
            var results = new List<object>();
            int start = 0;

            var url = initialUrl;

            while (true)
            {
                var paginatedUrl = url.Contains("start=")
                    ? url
                    : url + $"&start={start}";

                var json = await SendWithRetryAsync(paginatedUrl, accessToken);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // LinkedIn REST API returns data in an "elements" array
                if (root.TryGetProperty("elements", out var elements)
                    && elements.ValueKind == JsonValueKind.Array)
                {
                    int pageCount = 0;
                    foreach (var element in elements.EnumerateArray())
                    {
                        results.Add(ParseLinkedInObject(element));
                        pageCount++;
                    }

                    // If we got fewer results than the page size, we're done
                    if (pageCount < DefaultCount)
                        break;

                    start += pageCount;
                }
                else
                {
                    // No elements — might be a single-object response or empty
                    break;
                }
            }

            _logger.LogInformation("LinkedInAds: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── HTTP with retry / rate-limit handling ────────────────────────────

        private async Task<string> SendWithRetryAsync(string url, string accessToken)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("LinkedInAds rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"LinkedIn Ads API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "linkedinads");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("LinkedInAds API error {StatusCode}: {Body}",
                        (int)response.StatusCode, errorBody);

                    throw new ConnectorException(
                        $"LinkedIn Ads API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "linkedinads");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "LinkedIn Ads request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "linkedinads");
        }

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("LinkedIn-Version", "202402");
            request.Headers.Add("X-Restli-Protocol-Version", "2.0.0");
        }

        // ── JSON parsing ─────────────────────────────────────────────────────

        private static IDictionary<string, object> ParseLinkedInObject(JsonElement element)
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
                    $"LinkedIn Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "linkedinads");
            return value;
        }

        private string RequireAdAccountId(string? adAccountId)
        {
            if (string.IsNullOrWhiteSpace(adAccountId))
                throw new ConnectorException(
                    "LinkedIn Ads connector requires the 'adAccountId' parameter for this resource.",
                    new ArgumentException("Missing required parameter: adAccountId"),
                    "linkedinads");
            return adAccountId;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
