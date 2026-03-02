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
    /// Reads data from the WooCommerce REST API v3.
    ///
    /// Parameters:
    ///   consumerKey    — WooCommerce REST API consumer key
    ///   consumerSecret — WooCommerce REST API consumer secret
    ///   storeUrl       — WooCommerce store URL (e.g. "mystore.com" or "https://mystore.com")
    ///   resource       — orders, products, customers, coupons, categories, tags, shipping,
    ///                    taxes, reports, refunds, payment_gateways, webhooks
    ///
    /// ConnectionString can be JSON: {"storeUrl":"...","consumerKey":"...","consumerSecret":"..."}
    /// Parameters override ConnectionString values when both are present.
    /// </summary>
    public class WooCommerceReader : ISourceReader
    {
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "orders", "products", "customers", "coupons", "categories",
            "tags", "shipping", "taxes", "reports", "refunds",
            "payment_gateways", "webhooks"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<WooCommerceReader> _logger;

        public WooCommerceReader(HttpClient httpClient, ILogger<WooCommerceReader> logger)
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
            var (storeUrl, consumerKey, consumerSecret) = ResolveCredentials(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("WooCommerce: reading resource '{Resource}' from '{StoreUrl}'.", resource, storeUrl);

            try
            {
                string? extraQuery = null;
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    // WooCommerce supports modified_after for orders/products/customers.
                    extraQuery = $"modified_after={Uri.EscapeDataString(watermarkValue)}";
                }

                return await ReadFullAsync(storeUrl, consumerKey, consumerSecret, resource, extraQuery);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WooCommerce: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read WooCommerce resource '{resource}': {ex.Message}", ex, "woocommerce");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("WooCommerce: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await DryRunPreviewAsync(config, 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WooCommerce: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover WooCommerce schema for '{resource}': {ex.Message}", ex, "woocommerce");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (storeUrl, consumerKey, consumerSecret) = ResolveCredentials(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("WooCommerce: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                // Fetch only one page and take the sample.
                var records = await ReadFullAsync(storeUrl, consumerKey, consumerSecret, resource, extraQuery: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WooCommerce: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"WooCommerce dry run preview failed for '{resource}': {ex.Message}", ex, "woocommerce");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (page-based pagination) ────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string storeUrl, string consumerKey, string consumerSecret,
            string resource, string? extraQuery, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;
            int pagesRead = 0;
            int totalPages = int.MaxValue;

            do
            {
                var url = BuildUrl(storeUrl, resource, page, extraQuery);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, consumerKey, consumerSecret);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                // Extract total pages from response header.
                if (response.Headers.TryGetValues("X-WP-TotalPages", out var totalPagesValues))
                {
                    var tpStr = totalPagesValues.FirstOrDefault();
                    if (int.TryParse(tpStr, out var tp))
                        totalPages = tp;
                }

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var count = ParseArrayResponse(doc.RootElement, results);

                pagesRead++;
                page++;

                if (count == 0 || page > totalPages || pagesRead >= maxPages)
                    break;
            }
            while (true);

            _logger.LogInformation("WooCommerce: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pagesRead);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string storeUrl, string resource, int page, string? extraQuery)
        {
            var normalizedStore = NormalizeStoreUrl(storeUrl);
            var sb = new StringBuilder($"{normalizedStore}/wp-json/wc/v3/{resource}?per_page={PageSize}&page={page}");

            if (!string.IsNullOrEmpty(extraQuery))
            {
                sb.Append($"&{extraQuery}");
            }

            return sb.ToString();
        }

        private static string NormalizeStoreUrl(string storeUrl)
        {
            var url = storeUrl.TrimEnd('/');
            if (!url.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                && !url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                url = $"https://{url}";
            }
            return url;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a WooCommerce API response. The response is a JSON array directly.
        /// Returns the number of records found on this page.
        /// </summary>
        private static int ParseArrayResponse(JsonElement root, List<object> results)
        {
            if (root.ValueKind != JsonValueKind.Array)
                return 0;

            int count = 0;
            foreach (var element in root.EnumerateArray())
            {
                results.Add(JsonElementToExpando(element));
                count++;
            }

            return count;
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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string consumerKey, string consumerSecret)
        {
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{consumerKey}:{consumerSecret}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // ── Credential resolution ────────────────────────────────────────────

        /// <summary>
        /// Resolves WooCommerce credentials from Parameters and ConnectionString.
        /// ConnectionString can be JSON: {"storeUrl":"...","consumerKey":"...","consumerSecret":"..."}
        /// Parameters take precedence over ConnectionString values.
        /// </summary>
        private (string storeUrl, string consumerKey, string consumerSecret) ResolveCredentials(DataSourceConfig config)
        {
            // Try to parse ConnectionString as JSON first.
            string? connStoreUrl = null;
            string? connConsumerKey = null;
            string? connConsumerSecret = null;

            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    var root = doc.RootElement;
                    connStoreUrl = root.TryGetProperty("storeUrl", out var s) ? s.GetString() : null;
                    connConsumerKey = root.TryGetProperty("consumerKey", out var k) ? k.GetString() : null;
                    connConsumerSecret = root.TryGetProperty("consumerSecret", out var sec) ? sec.GetString() : null;
                }
                catch (JsonException)
                {
                    // ConnectionString is not JSON — ignore it for credential parsing.
                }
            }

            // Parameters override ConnectionString values.
            var storeUrl = GetStringParam(config.Parameters, "storeUrl")
                ?? connStoreUrl
                ?? throw new ConnectorException(
                    "WooCommerce storeUrl is required. Provide it via Parameters['storeUrl'] or the connection string JSON.",
                    new ArgumentException("Missing 'storeUrl'."),
                    "woocommerce");

            var consumerKey = GetStringParam(config.Parameters, "consumerKey")
                ?? connConsumerKey
                ?? throw new ConnectorException(
                    "WooCommerce consumerKey is required. Provide it via Parameters['consumerKey'] or the connection string JSON.",
                    new ArgumentException("Missing 'consumerKey'."),
                    "woocommerce");

            var consumerSecret = GetStringParam(config.Parameters, "consumerSecret")
                ?? connConsumerSecret
                ?? throw new ConnectorException(
                    "WooCommerce consumerSecret is required. Provide it via Parameters['consumerSecret'] or the connection string JSON.",
                    new ArgumentException("Missing 'consumerSecret'."),
                    "woocommerce");

            return (storeUrl, consumerKey, consumerSecret);
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
                    $"WooCommerce connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "woocommerce");
            return value;
        }
    }
}
