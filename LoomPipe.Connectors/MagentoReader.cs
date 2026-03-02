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
    /// Reads data from Magento (Adobe Commerce) using the REST API v1.
    ///
    /// Parameters:
    ///   accessToken  — Magento integration access token (Bearer token)
    ///   baseUrl      — Magento store base URL (e.g. https://mystore.com)
    ///   resource     — products, orders, customers, categories, cmsPages, cmsBlocks,
    ///                  invoices, shipments, creditMemos, stockItems, coupons
    /// </summary>
    public class MagentoReader : ISourceReader
    {
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "products", "orders", "customers", "categories", "cmsPages", "cmsBlocks",
            "invoices", "shipments", "creditMemos", "stockItems", "coupons"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MagentoReader> _logger;

        public MagentoReader(HttpClient httpClient, ILogger<MagentoReader> logger)
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
                    "Magento access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "magento");
            var baseUrl = GetRequiredParam(config.Parameters, "baseUrl");

            _logger.LogInformation("Magento: reading resource '{Resource}' from '{BaseUrl}'.", resource, baseUrl);

            try
            {
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    return await ReadFilteredAsync(resource, accessToken, baseUrl, watermarkField, watermarkValue);
                }

                return await ReadFullAsync(resource, accessToken, baseUrl);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Magento: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Magento resource '{resource}': {ex.Message}", ex, "magento");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Magento access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "magento");
            var baseUrl = GetRequiredParam(config.Parameters, "baseUrl");

            _logger.LogInformation("Magento: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, baseUrl, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Magento: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Magento schema for '{resource}': {ex.Message}", ex, "magento");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Magento access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "magento");
            var baseUrl = GetRequiredParam(config.Parameters, "baseUrl");

            _logger.LogInformation("Magento: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, baseUrl, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Magento: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Magento dry run preview failed for '{resource}': {ex.Message}", ex, "magento");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string baseUrl, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int currentPage = 1;
            int totalCount = int.MaxValue;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, baseUrl, currentPage);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                totalCount = ParseResultsPage(doc.RootElement, results);

                currentPage++;
                page++;
            }
            while (results.Count < totalCount && page < maxPages);

            _logger.LogInformation("Magento: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Filtered read (with searchCriteria filter) ──────────────────────

        private async Task<List<object>> ReadFilteredAsync(
            string resource, string accessToken, string baseUrl,
            string filterField, string filterValue)
        {
            var results = new List<object>();
            int currentPage = 1;
            int totalCount = int.MaxValue;

            do
            {
                var url = BuildFilteredUrl(resource, baseUrl, currentPage, filterField, filterValue);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                totalCount = ParseResultsPage(doc.RootElement, results);

                currentPage++;
            }
            while (results.Count < totalCount);

            _logger.LogInformation("Magento: incremental read returned {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── URL builders ────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string baseUrl, int currentPage)
        {
            var endpoint = GetEndpoint(resource);
            var normalizedBase = baseUrl.TrimEnd('/');

            return $"{normalizedBase}/rest/V1/{endpoint}?searchCriteria[pageSize]={PageSize}&searchCriteria[currentPage]={currentPage}";
        }

        private static string BuildFilteredUrl(
            string resource, string baseUrl, int currentPage, string filterField, string filterValue)
        {
            var endpoint = GetEndpoint(resource);
            var normalizedBase = baseUrl.TrimEnd('/');

            var sb = new StringBuilder();
            sb.Append($"{normalizedBase}/rest/V1/{endpoint}");
            sb.Append($"?searchCriteria[pageSize]={PageSize}");
            sb.Append($"&searchCriteria[currentPage]={currentPage}");
            sb.Append($"&searchCriteria[filter_groups][0][filters][0][field]={Uri.EscapeDataString(filterField)}");
            sb.Append($"&searchCriteria[filter_groups][0][filters][0][value]={Uri.EscapeDataString(filterValue)}");
            sb.Append($"&searchCriteria[filter_groups][0][filters][0][condition_type]=gteq");

            return sb.ToString();
        }

        /// <summary>
        /// Maps a resource name to the Magento REST API endpoint path segment.
        /// </summary>
        private static string GetEndpoint(string resource) => resource switch
        {
            "products"    => "products",
            "orders"      => "orders",
            "customers"   => "customers/search",
            "categories"  => "categories/list",
            "cmsPages"    => "cmsPage/search",
            "cmsBlocks"   => "cmsBlock/search",
            "invoices"    => "invoices",
            "shipments"   => "shipments",
            "creditMemos" => "creditmemos",
            "stockItems"  => "stockItems/lowStock",
            "coupons"     => "coupons/search",
            _ => throw new ConnectorException(
                $"Magento: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                new ArgumentException($"Unsupported resource: {resource}"),
                "magento")
        };

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a Magento search response: { "items": [...], "total_count": N }.
        /// Returns total_count for pagination control.
        /// </summary>
        private static int ParseResultsPage(JsonElement root, List<object> results)
        {
            int totalCount = int.MaxValue;

            if (root.TryGetProperty("total_count", out var tc) && tc.ValueKind == JsonValueKind.Number)
            {
                totalCount = tc.GetInt32();
            }

            JsonElement items;

            if (root.TryGetProperty("items", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Magento search response
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return totalCount;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                results.Add(row);
            }

            return totalCount;
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
                    $"Magento connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "magento");
            return value;
        }
    }
}
