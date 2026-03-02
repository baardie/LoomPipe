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
    /// Reads data from the BigCommerce API v3.
    ///
    /// Parameters:
    ///   accessToken  — BigCommerce API token (X-Auth-Token)
    ///   storeHash    — BigCommerce store hash (e.g. "abc123")
    ///   resource     — products, orders, customers, categories, brands, coupons,
    ///                  gift_certificates, redirects, pages
    /// </summary>
    public class BigCommerceReader : ISourceReader
    {
        private const int PageLimit = 250;

        private static readonly string[] AllResources =
        {
            "products", "orders", "customers", "categories", "brands",
            "coupons", "gift_certificates", "redirects", "pages"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<BigCommerceReader> _logger;

        public BigCommerceReader(HttpClient httpClient, ILogger<BigCommerceReader> logger)
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
                    "BigCommerce API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bigcommerce");
            var storeHash   = GetRequiredParam(config.Parameters, "storeHash");

            _logger.LogInformation("BigCommerce: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, storeHash);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BigCommerce: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read BigCommerce resource '{resource}': {ex.Message}", ex, "bigcommerce");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "BigCommerce API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bigcommerce");
            var storeHash   = GetRequiredParam(config.Parameters, "storeHash");

            _logger.LogInformation("BigCommerce: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, storeHash, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BigCommerce: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover BigCommerce schema for '{resource}': {ex.Message}", ex, "bigcommerce");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "BigCommerce API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bigcommerce");
            var storeHash   = GetRequiredParam(config.Parameters, "storeHash");

            _logger.LogInformation("BigCommerce: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, storeHash, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BigCommerce: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"BigCommerce dry run preview failed for '{resource}': {ex.Message}", ex, "bigcommerce");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (page-based pagination) ────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string storeHash, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var baseUrl = $"https://api.bigcommerce.com/stores/{Uri.EscapeDataString(storeHash)}";
            int pageNum = 1;
            int page = 0;
            int totalPages = int.MaxValue;

            do
            {
                var url = BuildUrl(resource, baseUrl, pageNum);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, resource, results);

                // Pagination: meta.pagination.total_pages
                if (doc.RootElement.TryGetProperty("meta", out var meta)
                    && meta.TryGetProperty("pagination", out var pagination)
                    && pagination.TryGetProperty("total_pages", out var tp)
                    && tp.ValueKind == JsonValueKind.Number)
                {
                    totalPages = tp.GetInt32();
                }

                pageNum++;
                page++;
            }
            while (pageNum <= totalPages && page < maxPages);

            _logger.LogInformation("BigCommerce: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string baseUrl, int pageNum)
        {
            var endpoint = resource switch
            {
                "products"          => "/v3/catalog/products",
                "orders"            => "/v2/orders",
                "customers"         => "/v3/customers",
                "categories"        => "/v3/catalog/categories",
                "brands"            => "/v3/catalog/brands",
                "coupons"           => "/v2/coupons",
                "gift_certificates" => "/v2/gift_certificates",
                "redirects"         => "/v3/storefront/redirects",
                "pages"             => "/v3/content/pages",
                _ => throw new ConnectorException(
                    $"BigCommerce: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "bigcommerce")
            };

            // v2 endpoints use "page" and "limit"; v3 use the same.
            return $"{baseUrl}{endpoint}?limit={PageLimit}&page={pageNum}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            JsonElement items;

            // v3 endpoints wrap data in "data": [...]
            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard v3 response shape
            }
            // v2 endpoints return a top-level array
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten one level of nested objects (e.g. "custom_url", "images").
                        foreach (var nested in prop.Value.EnumerateObject())
                        {
                            if (nested.Value.ValueKind == JsonValueKind.Object || nested.Value.ValueKind == JsonValueKind.Array)
                                row[$"{prop.Name}_{nested.Name}"] = nested.Value.ToString();
                            else
                                row[$"{prop.Name}_{nested.Name}"] = ConvertJsonValue(nested.Value);
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        row[prop.Name] = prop.Value.ToString();
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
            request.Headers.Add("X-Auth-Token", accessToken);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
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
                    $"BigCommerce connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "bigcommerce");
            return value;
        }
    }
}
