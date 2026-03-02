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
    /// Reads data from Salesforce Commerce Cloud (SFCC) using the OCAPI Data API (v24_5).
    ///
    /// Parameters:
    ///   accessToken — Bearer token (obtained via OAuth client credentials at /dw/oauth2/access_token)
    ///   host        — SFCC sandbox or production hostname (e.g. "my-sandbox.demandware.net")
    ///   resource    — products, categories, orders, customers, inventory, content,
    ///                 campaigns, promotions, price_books, catalogs
    ///   siteId      — optional site identifier (defaults to "-")
    ///
    /// ConnectionString JSON: {"host":"...","accessToken":"...","siteId":"..."}
    /// </summary>
    public class SfccReader : ISourceReader
    {
        private const string ApiVersion = "v24_5";
        private const int PageSize = 200;

        private static readonly string[] AllResources =
        {
            "products", "categories", "orders", "customers", "inventory",
            "content", "campaigns", "promotions", "price_books", "catalogs"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SfccReader> _logger;

        public SfccReader(HttpClient httpClient, ILogger<SfccReader> logger)
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
            var parameters  = MergeConnectionString(config);
            var host        = GetRequiredParam(parameters, "host");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var siteId      = GetStringParam(parameters, "siteId") ?? "-";

            _logger.LogInformation("SFCC: reading resource '{Resource}' from host '{Host}'.", resource, host);

            try
            {
                return await ReadFullAsync(host, accessToken, resource, siteId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SFCC: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read SFCC resource '{resource}': {ex.Message}", ex, "sfcc");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var host        = GetRequiredParam(parameters, "host");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var siteId      = GetStringParam(parameters, "siteId") ?? "-";

            _logger.LogInformation("SFCC: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(host, accessToken, resource, siteId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SFCC: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover SFCC schema for '{resource}': {ex.Message}", ex, "sfcc");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var host        = GetRequiredParam(parameters, "host");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var siteId      = GetStringParam(parameters, "siteId") ?? "-";

            _logger.LogInformation("SFCC: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(host, accessToken, resource, siteId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SFCC: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"SFCC dry run preview failed for '{resource}': {ex.Message}", ex, "sfcc");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string host, string accessToken, string resource, string siteId,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            do
            {
                var url = BuildUrl(host, resource, siteId, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var (items, total) = ParseResultsPage(doc.RootElement, resource);
                results.AddRange(items);

                offset += PageSize;
                page++;

                // Stop when we've fetched all records or hit the page limit
                if (items.Count < PageSize || offset >= total || page >= maxPages)
                    break;
            }
            while (true);

            _logger.LogInformation("SFCC: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string host, string resource, string siteId, int offset)
        {
            var baseUrl = $"https://{host}/s/{siteId}/dw/data/{ApiVersion}";

            return resource switch
            {
                "products"   => $"{baseUrl}/product_search?count={PageSize}&start={offset}&select=(**)",
                "categories" => $"{baseUrl}/catalogs/storefront/categories?count={PageSize}&start={offset}",
                "orders"     => $"{baseUrl}/order_search?count={PageSize}&start={offset}",
                "customers"  => $"{baseUrl}/customer_search?count={PageSize}&start={offset}",
                "inventory"  => $"{baseUrl}/inventory_lists?count={PageSize}&start={offset}",
                "content"    => $"{baseUrl}/content_search?count={PageSize}&start={offset}",
                "campaigns"  => $"{baseUrl}/campaigns?count={PageSize}&start={offset}",
                "promotions" => $"{baseUrl}/promotions?count={PageSize}&start={offset}",
                "price_books" => $"{baseUrl}/price_books?count={PageSize}&start={offset}",
                "catalogs"   => $"{baseUrl}/catalogs?count={PageSize}&start={offset}",
                _            => $"{baseUrl}/{resource}?count={PageSize}&start={offset}"
            };
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static (List<object> Items, int Total) ParseResultsPage(JsonElement root, string resource)
        {
            var items = new List<object>();
            int total = 0;

            // SFCC responses: { "hits": [...], "count": N, "start": 0, "total": N }
            // or { "data": [...], "count": N, "total": N }
            if (root.TryGetProperty("total", out var totalEl) && totalEl.ValueKind == JsonValueKind.Number)
                total = totalEl.GetInt32();

            JsonElement arrayElement = default;

            if (root.TryGetProperty("hits", out var hits) && hits.ValueKind == JsonValueKind.Array)
            {
                arrayElement = hits;
            }
            else if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
            {
                arrayElement = data;
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                arrayElement = root;
            }
            else
            {
                return (items, total);
            }

            foreach (var element in arrayElement.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                items.Add(row);
            }

            return (items, total);
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

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(token))
                return token;

            throw new ConnectorException(
                "SFCC access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                new ArgumentException("Missing 'accessToken'."),
                "sfcc");
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"host":"...","accessToken":"...","siteId":"..."}
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
                    $"SFCC connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "sfcc");
            return value;
        }
    }
}
