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
    /// Reads CMS data from Webflow using the v2 API.
    ///
    /// Parameters:
    ///   accessToken   — Webflow API bearer token
    ///   resource      — sites, collections, items, users, products, orders,
    ///                   inventory, memberships
    ///   siteId        — required for collections, users, products, orders, memberships
    ///   collectionId  — required for items
    /// </summary>
    public class WebflowReader : ISourceReader
    {
        private const string BaseUrl = "https://api.webflow.com/v2";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "sites", "collections", "items", "users",
            "products", "orders", "inventory", "memberships"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<WebflowReader> _logger;

        public WebflowReader(HttpClient httpClient, ILogger<WebflowReader> logger)
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
                    "Webflow access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "webflow");
            var siteId       = GetStringParam(config.Parameters, "siteId");
            var collectionId = GetStringParam(config.Parameters, "collectionId");

            _logger.LogInformation("Webflow: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, siteId, collectionId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Webflow: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Webflow resource '{resource}': {ex.Message}", ex, "webflow");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Webflow access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "webflow");
            var siteId       = GetStringParam(config.Parameters, "siteId");
            var collectionId = GetStringParam(config.Parameters, "collectionId");

            _logger.LogInformation("Webflow: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, siteId, collectionId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Webflow: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Webflow schema for '{resource}': {ex.Message}", ex, "webflow");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Webflow access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "webflow");
            var siteId       = GetStringParam(config.Parameters, "siteId");
            var collectionId = GetStringParam(config.Parameters, "collectionId");

            _logger.LogInformation("Webflow: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, siteId, collectionId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Webflow: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Webflow dry run preview failed for '{resource}': {ex.Message}", ex, "webflow");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? siteId, string? collectionId,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, siteId, collectionId, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var itemsKey = GetItemsKey(resource);
                int count = 0;

                if (doc.RootElement.TryGetProperty(itemsKey, out var items)
                    && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                        count++;
                    }
                }
                else if (doc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in doc.RootElement.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                        count++;
                    }
                }

                // Pagination: check total vs offset
                int total = 0;
                if (doc.RootElement.TryGetProperty("total", out var totalEl)
                    && totalEl.ValueKind == JsonValueKind.Number)
                {
                    total = totalEl.GetInt32();
                }

                offset += count;
                page++;

                if (count == 0 || offset >= total)
                    break;
            }
            while (page < maxPages);

            _logger.LogInformation("Webflow: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? siteId, string? collectionId, int offset)
        {
            string baseEndpoint = resource switch
            {
                "sites"       => $"{BaseUrl}/sites",
                "collections" => $"{BaseUrl}/sites/{siteId}/collections",
                "items"       => $"{BaseUrl}/collections/{collectionId}/items",
                "users"       => $"{BaseUrl}/sites/{siteId}/users",
                "products"    => $"{BaseUrl}/sites/{siteId}/products",
                "orders"      => $"{BaseUrl}/sites/{siteId}/orders",
                "inventory"   => $"{BaseUrl}/sites/{siteId}/inventory",
                "memberships" => $"{BaseUrl}/sites/{siteId}/memberships",
                _             => $"{BaseUrl}/sites/{siteId}/{resource}"
            };

            var sb = new StringBuilder(baseEndpoint);
            sb.Append($"?limit={PageLimit}");
            sb.Append($"&offset={offset}");

            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON property name containing the array of items for a given resource.
        /// </summary>
        private static string GetItemsKey(string resource) => resource switch
        {
            "sites"       => "sites",
            "collections" => "collections",
            "items"       => "items",
            "users"       => "users",
            "products"    => "items",
            "orders"      => "orders",
            "inventory"   => "items",
            "memberships" => "users",
            _             => "items"
        };

        // ── Response parsing ─────────────────────────────────────────────────

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
                    $"Webflow connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "webflow");
            return value;
        }
    }
}
