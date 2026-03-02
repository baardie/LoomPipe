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
    /// Reads advertising data from Google Ads using the REST API (v17).
    ///
    /// Parameters:
    ///   accessToken    — OAuth 2.0 access token
    ///   developerToken — Google Ads API developer token
    ///   customerId     — 10-digit customer ID (no dashes)
    ///   resource       — campaigns, ad_groups, ads, keywords, search_terms, conversions,
    ///                    ad_group_criteria, campaign_budgets, bidding_strategies, extensions
    ///   query          — optional GAQL query override
    /// </summary>
    public class GoogleAdsReader : ISourceReader
    {
        private const string BaseUrl = "https://googleads.googleapis.com/v17";
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "campaigns", "ad_groups", "ads", "keywords", "search_terms",
            "conversions", "ad_group_criteria", "campaign_budgets",
            "bidding_strategies", "extensions"
        };

        /// <summary>
        /// Default GAQL queries per resource. These select the most commonly useful fields.
        /// </summary>
        private static readonly Dictionary<string, string> DefaultQueries = new(StringComparer.OrdinalIgnoreCase)
        {
            ["campaigns"] =
                "SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, " +
                "metrics.impressions, metrics.clicks, metrics.cost_micros FROM campaign",
            ["ad_groups"] =
                "SELECT ad_group.id, ad_group.name, ad_group.status, ad_group.campaign, " +
                "metrics.impressions, metrics.clicks, metrics.cost_micros FROM ad_group",
            ["ads"] =
                "SELECT ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.status, " +
                "ad_group_ad.ad.final_urls, metrics.impressions, metrics.clicks, metrics.cost_micros FROM ad_group_ad",
            ["keywords"] =
                "SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, " +
                "ad_group_criterion.keyword.match_type, ad_group_criterion.status, " +
                "metrics.impressions, metrics.clicks, metrics.cost_micros FROM keyword_view",
            ["search_terms"] =
                "SELECT search_term_view.search_term, search_term_view.resource_name, " +
                "segments.keyword.info.text, metrics.impressions, metrics.clicks, metrics.cost_micros FROM search_term_view",
            ["conversions"] =
                "SELECT conversion_action.id, conversion_action.name, conversion_action.type, " +
                "conversion_action.status, metrics.conversions, metrics.conversions_value FROM conversion_action",
            ["ad_group_criteria"] =
                "SELECT ad_group_criterion.criterion_id, ad_group_criterion.type, ad_group_criterion.status, " +
                "ad_group_criterion.ad_group, metrics.impressions, metrics.clicks FROM ad_group_criterion",
            ["campaign_budgets"] =
                "SELECT campaign_budget.id, campaign_budget.name, campaign_budget.amount_micros, " +
                "campaign_budget.status, campaign_budget.delivery_method FROM campaign_budget",
            ["bidding_strategies"] =
                "SELECT bidding_strategy.id, bidding_strategy.name, bidding_strategy.type, " +
                "bidding_strategy.status FROM bidding_strategy",
            ["extensions"] =
                "SELECT extension_feed_item.id, extension_feed_item.extension_type, " +
                "extension_feed_item.status FROM extension_feed_item"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleAdsReader> _logger;

        public GoogleAdsReader(HttpClient httpClient, ILogger<GoogleAdsReader> logger)
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

            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetRequiredParam(config.Parameters, "accessToken");
            var developerToken = GetRequiredParam(config.Parameters, "developerToken");
            var customerId     = GetRequiredParam(config.Parameters, "customerId");
            var query          = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("GoogleAds: reading resource '{Resource}' for customer {CustomerId}.", resource, customerId);

            try
            {
                // Build the GAQL query
                var gaql = !string.IsNullOrEmpty(query)
                    ? query
                    : DefaultQueries.TryGetValue(resource, out var defaultQuery)
                        ? defaultQuery
                        : throw new ConnectorException(
                            $"No default query for Google Ads resource '{resource}'. Provide a 'query' parameter with a valid GAQL statement.",
                            new ArgumentException($"Unknown resource: {resource}"),
                            "googleads");

                // Append watermark filter if provided
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    gaql += gaql.Contains(" WHERE ", StringComparison.OrdinalIgnoreCase)
                        ? $" AND {watermarkField} > '{watermarkValue}'"
                        : $" WHERE {watermarkField} > '{watermarkValue}'";
                }

                return await ExecuteSearchStreamAsync(customerId, accessToken, developerToken, gaql, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleAds: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Google Ads resource '{resource}': {ex.Message}", ex, "googleads");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("GoogleAds: discovering schema for '{Resource}'.", resource);

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
                _logger.LogError(ex, "GoogleAds: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Google Ads schema for '{resource}': {ex.Message}", ex, "googleads");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("GoogleAds: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                // Modify query to limit rows if possible
                var query = GetStringParam(config.Parameters, "query");
                if (string.IsNullOrEmpty(query) && DefaultQueries.TryGetValue(resource, out var defaultQuery))
                {
                    query = defaultQuery + $" LIMIT {sampleSize}";
                    config.Parameters["query"] = query;
                }

                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleAds: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"GoogleAds dry run preview failed for '{resource}': {ex.Message}", ex, "googleads");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Search Stream API ────────────────────────────────────────────────

        private async Task<List<object>> ExecuteSearchStreamAsync(
            string customerId, string accessToken, string developerToken,
            string gaqlQuery, string resource)
        {
            var url = $"{BaseUrl}/customers/{customerId}/googleAds:searchStream";
            var results = new List<object>();

            var requestBody = JsonSerializer.Serialize(new { query = gaqlQuery });

            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(requestBody, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken, developerToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("GoogleAds rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Google Ads API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "googleads");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("GoogleAds API error {StatusCode}: {Body}",
                        (int)response.StatusCode, errorBody);

                    throw new ConnectorException(
                        $"Google Ads API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "googleads");
                }

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Response is a JSON array of batch objects, each with a "results" array
                if (doc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var batch in doc.RootElement.EnumerateArray())
                    {
                        if (batch.TryGetProperty("results", out var batchResults)
                            && batchResults.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var element in batchResults.EnumerateArray())
                            {
                                results.Add(FlattenObject(element));
                            }
                        }
                    }
                }
                else if (doc.RootElement.TryGetProperty("results", out var singleResults)
                         && singleResults.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in singleResults.EnumerateArray())
                    {
                        results.Add(FlattenObject(element));
                    }
                }

                break;
            }

            _logger.LogInformation("GoogleAds: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── JSON flattening ─────────────────────────────────────────────────

        /// <summary>
        /// Recursively flattens nested JSON objects into a single-level ExpandoObject.
        /// Nested keys are joined with underscores: e.g. campaign.name → campaign_name.
        /// </summary>
        private static IDictionary<string, object> FlattenObject(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();
            FlattenRecursive(element, "", row);
            return row;
        }

        private static void FlattenRecursive(JsonElement element, string prefix, IDictionary<string, object> target)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in element.EnumerateObject())
                {
                    var key = string.IsNullOrEmpty(prefix) ? prop.Name : $"{prefix}_{prop.Name}";
                    FlattenRecursive(prop.Value, key, target);
                }
            }
            else
            {
                target[prefix] = ConvertJsonValue(element);
            }
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
                JsonValueKind.Array  => value.GetRawText(),
                _                    => value.ToString()
            };
        }

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string developerToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("developer-token", developerToken);
        }

        // ── Connection-string merge ─────────────────────────────────────────

        /// <summary>
        /// If ConnectionString is non-empty JSON, merges its properties into Parameters
        /// (Parameters take precedence).
        /// </summary>
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
                    $"Google Ads connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "googleads");
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
