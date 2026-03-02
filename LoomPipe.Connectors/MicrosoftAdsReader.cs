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
    /// Reads data from Microsoft Advertising (Bing Ads) using the Campaign Management
    /// and Reporting REST APIs (v13).
    ///
    /// Parameters:
    ///   accessToken    — Bearer token (Microsoft identity platform OAuth)
    ///   developerToken — Microsoft Advertising developer token
    ///   customerId     — Customer ID for the Microsoft Advertising account
    ///   accountId      — Account ID for the Microsoft Advertising account
    ///   resource       — campaigns, ad_groups, ads, keywords, reports, audiences, budgets
    ///
    /// ConnectionString JSON: {"accessToken":"...","developerToken":"...","customerId":"...","accountId":"..."}
    /// </summary>
    public class MicrosoftAdsReader : ISourceReader
    {
        private const string CampaignBaseUrl = "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13";
        private const string ReportingBaseUrl = "https://reporting.api.bingads.microsoft.com/Reporting/v13";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "campaigns", "ad_groups", "ads", "keywords",
            "reports", "audiences", "budgets"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MicrosoftAdsReader> _logger;

        public MicrosoftAdsReader(HttpClient httpClient, ILogger<MicrosoftAdsReader> logger)
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
            var parameters     = MergeConnectionString(config);
            var accessToken    = GetAccessToken(parameters, config);
            var developerToken = GetRequiredParam(parameters, "developerToken");
            var customerId     = GetRequiredParam(parameters, "customerId");
            var accountId      = GetRequiredParam(parameters, "accountId");
            var resource       = GetRequiredParam(parameters, "resource");

            _logger.LogInformation("MicrosoftAds: reading resource '{Resource}' for account '{AccountId}'.",
                resource, accountId);

            try
            {
                return await ReadFullAsync(accessToken, developerToken, customerId, accountId, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MicrosoftAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Microsoft Ads resource '{resource}': {ex.Message}", ex, "microsoftads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters     = MergeConnectionString(config);
            var accessToken    = GetAccessToken(parameters, config);
            var developerToken = GetRequiredParam(parameters, "developerToken");
            var customerId     = GetRequiredParam(parameters, "customerId");
            var accountId      = GetRequiredParam(parameters, "accountId");
            var resource       = GetRequiredParam(parameters, "resource");

            _logger.LogInformation("MicrosoftAds: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(accessToken, developerToken, customerId, accountId, resource, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MicrosoftAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Microsoft Ads schema for '{resource}': {ex.Message}", ex, "microsoftads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters     = MergeConnectionString(config);
            var accessToken    = GetAccessToken(parameters, config);
            var developerToken = GetRequiredParam(parameters, "developerToken");
            var customerId     = GetRequiredParam(parameters, "customerId");
            var accountId      = GetRequiredParam(parameters, "accountId");
            var resource       = GetRequiredParam(parameters, "resource");

            _logger.LogInformation("MicrosoftAds: dry run preview for '{Resource}' (sample={SampleSize}).",
                resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(accessToken, developerToken, customerId, accountId, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MicrosoftAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Microsoft Ads dry run preview failed for '{resource}': {ex.Message}", ex, "microsoftads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string accessToken, string developerToken,
            string customerId, string accountId, string resource,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageIndex = 0;
            int page = 0;

            do
            {
                var (url, body) = BuildRequest(resource, accountId, pageIndex);

                using var request = new HttpRequestMessage(
                    body != null ? HttpMethod.Post : HttpMethod.Get, url);

                ApplyAuth(request, accessToken, developerToken, customerId, accountId);

                if (body != null)
                {
                    request.Content = new StringContent(body, Encoding.UTF8, "application/json");
                }

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var items = ParseResultsPage(doc.RootElement, resource);
                results.AddRange(items);

                page++;
                pageIndex++;

                // Stop when we get fewer items than page size or hit max pages
                if (items.Count < PageSize || page >= maxPages)
                    break;
            }
            while (true);

            _logger.LogInformation("MicrosoftAds: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Request builder ─────────────────────────────────────────────────

        private static (string Url, string? Body) BuildRequest(string resource, string accountId, int pageIndex)
        {
            return resource switch
            {
                "campaigns" => (
                    $"{CampaignBaseUrl}/Campaigns/QueryByAccountId",
                    BuildQueryBody("Campaign", accountId, pageIndex)),

                "ad_groups" => (
                    $"{CampaignBaseUrl}/AdGroups/QueryByAccountId",
                    BuildQueryBody("AdGroup", accountId, pageIndex)),

                "ads" => (
                    $"{CampaignBaseUrl}/Ads/QueryByAccountId",
                    BuildQueryBody("Ad", accountId, pageIndex)),

                "keywords" => (
                    $"{CampaignBaseUrl}/Keywords/QueryByAccountId",
                    BuildQueryBody("Keyword", accountId, pageIndex)),

                "audiences" => (
                    $"{CampaignBaseUrl}/Audiences/QueryByAccountId",
                    BuildQueryBody("Audience", accountId, pageIndex)),

                "budgets" => (
                    $"{CampaignBaseUrl}/Budgets/QueryByAccountId",
                    BuildQueryBody("Budget", accountId, pageIndex)),

                "reports" => (
                    $"{ReportingBaseUrl}/GenerateReport",
                    BuildReportBody(accountId)),

                _ => throw new ConnectorException(
                    $"Microsoft Ads: unsupported resource '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "microsoftads")
            };
        }

        private static string BuildQueryBody(string entityType, string accountId, int pageIndex)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteString("AccountId", accountId);
            writer.WritePropertyName("PageInfo");
            writer.WriteStartObject();
            writer.WriteNumber("Index", pageIndex);
            writer.WriteNumber("Size", PageSize);
            writer.WriteEndObject();
            writer.WriteString("ReturnAdditionalFields", entityType);
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static string BuildReportBody(string accountId)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WritePropertyName("ReportRequest");
            writer.WriteStartObject();
            writer.WriteString("ReportName", "LoomPipe_AccountPerformance");
            writer.WriteString("Format", "Json");
            writer.WriteString("ReportType", "AccountPerformanceReportRequest");
            writer.WritePropertyName("Scope");
            writer.WriteStartObject();
            writer.WritePropertyName("AccountIds");
            writer.WriteStartArray();
            writer.WriteStringValue(accountId);
            writer.WriteEndArray();
            writer.WriteEndObject();
            writer.WritePropertyName("Time");
            writer.WriteStartObject();
            writer.WriteString("PredefinedTime", "Last30Days");
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static List<object> ParseResultsPage(JsonElement root, string resource)
        {
            var items = new List<object>();

            // Try common response shapes:
            // { "Campaigns": [...] }, { "AdGroups": [...] }, { "value": [...] }
            JsonElement arrayElement = default;
            bool found = false;

            // Map resource names to expected response property names
            var responseKey = resource switch
            {
                "campaigns" => "Campaigns",
                "ad_groups" => "AdGroups",
                "ads"       => "Ads",
                "keywords"  => "Keywords",
                "audiences" => "Audiences",
                "budgets"   => "Budgets",
                _           => null
            };

            if (responseKey != null && root.TryGetProperty(responseKey, out var keyedArray)
                && keyedArray.ValueKind == JsonValueKind.Array)
            {
                arrayElement = keyedArray;
                found = true;
            }

            if (!found && root.TryGetProperty("value", out var valueArray)
                && valueArray.ValueKind == JsonValueKind.Array)
            {
                arrayElement = valueArray;
                found = true;
            }

            if (!found)
            {
                // Walk top-level properties to find any array
                foreach (var prop in root.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        arrayElement = prop.Value;
                        found = true;
                        break;
                    }
                }
            }

            if (!found)
            {
                // For reports or single-object responses, wrap the root
                if (root.ValueKind == JsonValueKind.Object)
                {
                    items.Add(FlattenElement(root));
                }
                return items;
            }

            foreach (var element in arrayElement.EnumerateArray())
            {
                items.Add(FlattenElement(element));
            }

            return items;
        }

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
            request.Headers.TryAddWithoutValidation("DeveloperToken", developerToken);
            request.Headers.TryAddWithoutValidation("CustomerId", customerId);
            request.Headers.TryAddWithoutValidation("CustomerAccountId", accountId);
        }

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(token))
                return token;

            throw new ConnectorException(
                "Microsoft Ads access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                new ArgumentException("Missing 'accessToken'."),
                "microsoftads");
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"accessToken":"...","developerToken":"...","customerId":"...","accountId":"..."}
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
            catch (JsonException) { /* connection string is not JSON — ignore */ }

            return merged;
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
                    $"Microsoft Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "microsoftads");
            return value;
        }
    }
}
