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
    /// Reads campaign and reporting data from Microsoft Advertising (Bing Ads) using the REST v13 API.
    ///
    /// Parameters:
    ///   accessToken    — Microsoft identity platform OAuth bearer token
    ///   developerToken — Bing Ads developer token
    ///   customerId     — Microsoft Advertising customer ID
    ///   accountId      — Microsoft Advertising account ID
    ///   resource       — campaigns, ad_groups, ads, keywords, search_terms, audiences, performance_reports
    ///   startDate      — optional ISO date for reporting (YYYY-MM-DD)
    ///   endDate        — optional ISO date for reporting (YYYY-MM-DD)
    /// </summary>
    public class BingAdsReader : ISourceReader
    {
        private const string CampaignBaseUrl = "https://campaign.api.bingads.microsoft.com/v13";
        private const string ReportingBaseUrl = "https://reporting.api.bingads.microsoft.com/Reporting/v13";
        private const int DefaultPageSize = 1000;

        private static readonly string[] AllResources =
        {
            "campaigns", "ad_groups", "ads", "keywords",
            "search_terms", "audiences", "performance_reports"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<BingAdsReader> _logger;

        public BingAdsReader(HttpClient httpClient, ILogger<BingAdsReader> logger)
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
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Bing Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bingads");
            var developerToken = GetRequiredParam(config.Parameters, "developerToken");
            var customerId     = GetRequiredParam(config.Parameters, "customerId");
            var accountId      = GetRequiredParam(config.Parameters, "accountId");
            var startDate      = GetStringParam(config.Parameters, "startDate");
            var endDate        = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("BingAds: reading resource '{Resource}'.", resource);

            try
            {
                var records = resource == "performance_reports"
                    ? await ReadReportAsync(accessToken, developerToken, customerId, accountId, startDate, endDate)
                    : await ReadFullAsync(resource, accessToken, developerToken, customerId, accountId);

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
                _logger.LogError(ex, "BingAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Bing Ads resource '{resource}': {ex.Message}", ex, "bingads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Bing Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bingads");
            var developerToken = GetRequiredParam(config.Parameters, "developerToken");
            var customerId     = GetRequiredParam(config.Parameters, "customerId");
            var accountId      = GetRequiredParam(config.Parameters, "accountId");

            _logger.LogInformation("BingAds: discovering schema for '{Resource}'.", resource);

            try
            {
                List<object> sample;
                if (resource == "performance_reports")
                {
                    var startDate = GetStringParam(config.Parameters, "startDate");
                    var endDate   = GetStringParam(config.Parameters, "endDate");
                    sample = await ReadReportAsync(accessToken, developerToken, customerId, accountId, startDate, endDate);
                }
                else
                {
                    sample = await ReadFullAsync(resource, accessToken, developerToken, customerId, accountId, maxRecords: DefaultPageSize);
                }

                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BingAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Bing Ads schema for '{resource}': {ex.Message}", ex, "bingads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Bing Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bingads");
            var developerToken = GetRequiredParam(config.Parameters, "developerToken");
            var customerId     = GetRequiredParam(config.Parameters, "customerId");
            var accountId      = GetRequiredParam(config.Parameters, "accountId");

            _logger.LogInformation("BingAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                List<object> records;
                if (resource == "performance_reports")
                {
                    var startDate = GetStringParam(config.Parameters, "startDate");
                    var endDate   = GetStringParam(config.Parameters, "endDate");
                    records = await ReadReportAsync(accessToken, developerToken, customerId, accountId, startDate, endDate);
                }
                else
                {
                    records = await ReadFullAsync(resource, accessToken, developerToken, customerId, accountId, maxRecords: DefaultPageSize);
                }

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BingAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Bing Ads dry run preview failed for '{resource}': {ex.Message}", ex, "bingads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Campaign management API read ─────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string developerToken,
            string customerId, string accountId, int maxRecords = int.MaxValue)
        {
            var results = new List<object>();
            int pageIndex = 0;

            do
            {
                var url = BuildCampaignUrl(resource, accountId, pageIndex);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, developerToken, customerId, accountId);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);

                int added = results.Count - countBefore;
                if (added == 0 || added < DefaultPageSize) break;

                pageIndex++;
            }
            while (results.Count < maxRecords);

            _logger.LogInformation("BingAds: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── Reporting API read ───────────────────────────────────────────────

        private async Task<List<object>> ReadReportAsync(
            string accessToken, string developerToken, string customerId,
            string accountId, string? startDate, string? endDate)
        {
            var results = new List<object>();

            // Default to last 30 days if no dates provided.
            var end   = endDate   ?? DateTime.UtcNow.ToString("yyyy-MM-dd");
            var start = startDate ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");

            var reportBody = JsonSerializer.Serialize(new
            {
                ReportRequest = new
                {
                    ExcludeColumnHeaders = false,
                    ExcludeReportFooter = true,
                    ExcludeReportHeader = true,
                    Format = "Csv",
                    ReturnOnlyCompleteData = false,
                    Type = "CampaignPerformanceReportRequest",
                    Scope = new
                    {
                        AccountIds = new[] { long.Parse(accountId) }
                    },
                    Time = new
                    {
                        CustomDateRangeStart = new { Day = int.Parse(start.Substring(8, 2)), Month = int.Parse(start.Substring(5, 2)), Year = int.Parse(start.Substring(0, 4)) },
                        CustomDateRangeEnd = new { Day = int.Parse(end.Substring(8, 2)), Month = int.Parse(end.Substring(5, 2)), Year = int.Parse(end.Substring(0, 4)) }
                    },
                    Columns = new[]
                    {
                        "TimePeriod", "CampaignName", "CampaignId",
                        "Impressions", "Clicks", "Spend", "Conversions", "Revenue"
                    }
                }
            });

            var url = $"{ReportingBaseUrl}/GenerateReport";

            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(reportBody, Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken, developerToken, customerId, accountId);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            // The reporting API returns a report request ID for async polling,
            // or in some cases inline results. Parse whatever we get.
            ParseResultsPage(doc.RootElement, results);

            _logger.LogInformation("BingAds: report returned {Count} records.", results.Count);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildCampaignUrl(string resource, string accountId, int pageIndex)
        {
            var escapedAccountId = Uri.EscapeDataString(accountId);

            var endpoint = resource switch
            {
                "campaigns"    => $"{CampaignBaseUrl}/Campaigns",
                "ad_groups"    => $"{CampaignBaseUrl}/AdGroups",
                "ads"          => $"{CampaignBaseUrl}/Ads",
                "keywords"     => $"{CampaignBaseUrl}/Keywords",
                "search_terms" => $"{CampaignBaseUrl}/SearchTerms",
                "audiences"    => $"{CampaignBaseUrl}/Audiences",
                _ => throw new ConnectorException(
                    $"Unknown Bing Ads resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "bingads")
            };

            var sb = new StringBuilder(endpoint);
            sb.Append($"?AccountId={escapedAccountId}");
            sb.Append($"&PageSize={DefaultPageSize}");
            sb.Append($"&PageIndex={pageIndex}");

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            // Bing Ads wraps results in various property names depending on the endpoint.
            if (root.TryGetProperty("value", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // OData-style response
            }
            else if (root.TryGetProperty("Campaigns", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Campaign-specific wrapper
            }
            else if (root.TryGetProperty("AdGroups", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // AdGroup-specific wrapper
            }
            else if (root.TryGetProperty("Ads", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Ad-specific wrapper
            }
            else if (root.TryGetProperty("Keywords", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Keyword-specific wrapper
            }
            else if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Generic results
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                // Single object or report metadata — flatten it.
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

        private static void ApplyAuth(
            HttpRequestMessage request, string accessToken,
            string developerToken, string customerId, string accountId)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("DeveloperToken", developerToken);
            request.Headers.Add("CustomerId", customerId);
            request.Headers.Add("CustomerAccountId", accountId);
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
                    $"Bing Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "bingads");
            return value;
        }
    }
}
