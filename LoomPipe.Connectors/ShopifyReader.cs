#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from the Shopify REST Admin API.
    /// Uses link-header pagination, rate-limit retry, and incremental watermark support.
    /// </summary>
    public class ShopifyReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<ShopifyReader> _logger;

        private const string DefaultApiVersion = "2024-10";
        private const int MaxPageSize = 250;
        private const int MaxRetries = 3;

        private static readonly string[] SupportedResources =
        [
            "orders", "products", "customers", "inventory_items",
            "collections", "smart_collections", "custom_collections",
            "draft_orders", "abandoned_checkouts", "locations",
            "pages", "blogs", "articles", "metafields",
            "price_rules", "discount_codes"
        ];

        public ShopifyReader(HttpClient httpClient, ILogger<ShopifyReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ─────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var (shop, connStrToken) = ParseConnectionString(config.ConnectionString);
            var resource = GetStringParam(config.Parameters, "resource")
                ?? throw new ConnectorException(
                    "Shopify resource parameter is required.",
                    new ArgumentException("Missing 'resource' parameter."),
                    "shopify");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? connStrToken
                ?? throw new ConnectorException(
                    "Shopify accessToken parameter is required.",
                    new ArgumentException("Missing 'accessToken' parameter."),
                    "shopify");
            var apiVersion = GetStringParam(config.Parameters, "apiVersion") ?? DefaultApiVersion;
            var startDate = GetStringParam(config.Parameters, "startDate");
            var fields = GetStringParam(config.Parameters, "fields");

            _logger.LogInformation(
                "Reading Shopify resource '{Resource}' from shop '{Shop}' (api={ApiVersion}).",
                resource, shop, apiVersion);

            try
            {
                var url = BuildUrl(shop, apiVersion, resource, watermarkValue, startDate, fields);
                var allRecords = new List<object>();

                while (!string.IsNullOrEmpty(url))
                {
                    var (records, nextUrl) = await FetchPageAsync(url, accessToken, resource);
                    allRecords.AddRange(records);
                    url = nextUrl;
                }

                _logger.LogInformation(
                    "Read {Count} records from Shopify resource '{Resource}'.",
                    allRecords.Count, resource);

                return allRecords;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read Shopify resource '{Resource}' from '{Shop}'.", resource, shop);
                throw new ConnectorException($"Failed to read Shopify resource '{resource}': {ex.Message}", ex, "shopify");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for Shopify resource.");
            try
            {
                var records = await DryRunPreviewAsync(config, 1);
                var first = records.FirstOrDefault();
                if (first is IDictionary<string, object> dict)
                    return dict.Keys;
                return Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover Shopify schema.");
                throw new ConnectorException($"Failed to discover Shopify schema: {ex.Message}", ex, "shopify");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (shop, connStrToken) = ParseConnectionString(config.ConnectionString);
            var resource = GetStringParam(config.Parameters, "resource")
                ?? throw new ConnectorException(
                    "Shopify resource parameter is required.",
                    new ArgumentException("Missing 'resource' parameter."),
                    "shopify");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? connStrToken
                ?? throw new ConnectorException(
                    "Shopify accessToken parameter is required.",
                    new ArgumentException("Missing 'accessToken' parameter."),
                    "shopify");
            var apiVersion = GetStringParam(config.Parameters, "apiVersion") ?? DefaultApiVersion;
            var fields = GetStringParam(config.Parameters, "fields");

            _logger.LogInformation(
                "Dry run preview for Shopify resource '{Resource}' (limit={Limit}).",
                resource, sampleSize);

            try
            {
                var clampedSize = Math.Min(sampleSize, MaxPageSize);
                var url = BuildUrl(shop, apiVersion, resource, limit: clampedSize, fields: fields);
                var (records, _) = await FetchPageAsync(url, accessToken, resource);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for Shopify resource '{Resource}'.", resource);
                throw new ConnectorException($"Shopify dry run failed: {ex.Message}", ex, "shopify");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(SupportedResources);
        }

        // ── URL builder ───────────────────────────────────────────────────────

        private static string BuildUrl(
            string shop,
            string apiVersion,
            string resource,
            string? watermarkValue = null,
            string? startDate = null,
            string? fields = null,
            int limit = MaxPageSize)
        {
            var baseUrl = $"https://{shop}/admin/api/{apiVersion}/{resource}.json";
            var queryParams = new List<string> { $"limit={limit}" };

            if (!string.IsNullOrEmpty(watermarkValue))
                queryParams.Add($"updated_at_min={Uri.EscapeDataString(watermarkValue)}");

            if (!string.IsNullOrEmpty(startDate))
                queryParams.Add($"created_at_min={Uri.EscapeDataString(startDate)}");

            if (!string.IsNullOrEmpty(fields))
                queryParams.Add($"fields={Uri.EscapeDataString(fields)}");

            return $"{baseUrl}?{string.Join("&", queryParams)}";
        }

        // ── Fetch a single page with rate-limit retry ─────────────────────────

        private async Task<(List<object> Records, string? NextUrl)> FetchPageAsync(
            string url, string accessToken, string resource)
        {
            for (var attempt = 0; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.TryAddWithoutValidation("X-Shopify-Access-Token", accessToken);

                using var response = await _httpClient.SendAsync(request);

                // Handle rate limiting (HTTP 429)
                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Shopify rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException("429 Too Many Requests"),
                            "shopify");
                    }

                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(2);
                    _logger.LogWarning(
                        "Shopify rate limit hit. Retrying in {Seconds}s (attempt {Attempt}/{Max}).",
                        retryAfter.TotalSeconds, attempt + 1, MaxRetries);
                    await Task.Delay(retryAfter);
                    continue;
                }

                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                var records = ParseResourceJson(json, resource);

                // Extract Link header for pagination
                var nextUrl = ExtractNextPageUrl(response);

                return (records, nextUrl);
            }

            // Should not reach here, but satisfy the compiler
            return (new List<object>(), null);
        }

        // ── JSON parsing ──────────────────────────────────────────────────────

        private static List<object> ParseResourceJson(string json, string resource)
        {
            using var doc = JsonDocument.Parse(json);
            var results = new List<object>();

            // Shopify wraps the response: {"orders": [...]} or {"products": [...]}
            if (!doc.RootElement.TryGetProperty(resource, out var arrayElement))
            {
                // Some resources use a different key; try to find any array property
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        arrayElement = prop.Value;
                        break;
                    }
                }

                if (arrayElement.ValueKind != JsonValueKind.Array)
                    return results;
            }

            foreach (var element in arrayElement.EnumerateArray())
            {
                results.Add(JsonElementToExpando(element));
            }

            return results;
        }

        private static object JsonElementToExpando(JsonElement element)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                return element.ValueKind switch
                {
                    JsonValueKind.String => (object)(element.GetString() ?? string.Empty),
                    JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
                    JsonValueKind.True   => true,
                    JsonValueKind.False  => false,
                    JsonValueKind.Null   => string.Empty,
                    _                    => element.ToString()
                };
            }

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
                    JsonValueKind.Array  => prop.Value.ToString(),
                    JsonValueKind.Object => JsonElementToExpando(prop.Value),
                    _                    => prop.Value.ToString()
                };
            }
            return expando;
        }

        // ── Link-header pagination ────────────────────────────────────────────

        private static string? ExtractNextPageUrl(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return null;

            var linkHeader = string.Join(", ", linkValues);
            // Match pattern: <url>; rel="next"
            var match = Regex.Match(linkHeader, @"<([^>]+)>;\s*rel=""next""");
            return match.Success ? match.Groups[1].Value : null;
        }

        // ── Parameter helper ──────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static (string shopDomain, string? accessToken) ParseConnectionString(string connectionString)
        {
            try
            {
                using var doc = JsonDocument.Parse(connectionString);
                var root = doc.RootElement;
                var shop = root.TryGetProperty("shopDomain", out var s) ? s.GetString() : null;
                var token = root.TryGetProperty("accessToken", out var t) ? t.GetString() : null;
                return (shop ?? connectionString, token);
            }
            catch (JsonException)
            {
                return (connectionString, null);
            }
        }
    }
}
