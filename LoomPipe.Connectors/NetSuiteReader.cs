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
    /// Reads records from NetSuite using the REST API with SuiteQL queries.
    ///
    /// Parameters:
    ///   accessToken  — NetSuite Token-Based Auth access token (Bearer)
    ///   accountId    — NetSuite account ID (e.g. "1234567" for 1234567.suitetalk.api.netsuite.com)
    ///   resource     — table/record type: customer, vendor, invoice, salesorder, etc.
    ///   query        — optional custom SuiteQL query (overrides default SELECT * FROM resource)
    ///
    /// ConnectionString JSON: {"accountId":"...","accessToken":"..."}
    /// </summary>
    public class NetSuiteReader : ISourceReader
    {
        private const int PageLimit = 1000;

        private static readonly string[] AllResources =
        {
            "customer", "vendor", "invoice", "salesorder", "purchaseorder",
            "employee", "item", "account", "transaction", "contact",
            "opportunity", "task", "note"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<NetSuiteReader> _logger;

        public NetSuiteReader(HttpClient httpClient, ILogger<NetSuiteReader> logger)
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
            var accountId   = ResolveAccountId(config);
            var accessToken = ResolveAccessToken(config);
            var customQuery = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("NetSuite: reading resource '{Resource}' from account '{AccountId}'.", resource, accountId);

            try
            {
                return await ReadPaginatedAsync(accountId, accessToken, resource, customQuery, watermarkField, watermarkValue);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NetSuite: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read NetSuite resource '{resource}': {ex.Message}", ex, "netsuite");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accountId   = ResolveAccountId(config);
            var accessToken = ResolveAccessToken(config);

            _logger.LogInformation("NetSuite: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadPaginatedAsync(accountId, accessToken, resource, customQuery: null, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NetSuite: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover NetSuite schema for '{resource}': {ex.Message}", ex, "netsuite");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accountId   = ResolveAccountId(config);
            var accessToken = ResolveAccessToken(config);

            _logger.LogInformation("NetSuite: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(accountId, accessToken, resource, customQuery: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NetSuite: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"NetSuite dry run preview failed for '{resource}': {ex.Message}", ex, "netsuite");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read (SuiteQL POST) ────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string accountId, string accessToken, string resource,
            string? customQuery,
            string? watermarkField = null, string? watermarkValue = null,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;
            bool hasMore = true;

            var url = $"https://{accountId}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql";

            while (hasMore && page < maxPages)
            {
                var query = BuildSuiteQlQuery(resource, customQuery, watermarkField, watermarkValue, offset);
                var bodyJson = JsonSerializer.Serialize(new { q = query });

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(bodyJson, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);
                request.Headers.Add("Prefer", "transient");

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                // Check hasMore from response.
                hasMore = false;
                if (doc.RootElement.TryGetProperty("hasMore", out var hasMoreEl)
                    && hasMoreEl.ValueKind == JsonValueKind.True)
                {
                    hasMore = true;
                }

                page++;
                offset += PageLimit;

                if (fetched < PageLimit) break;
            }

            _logger.LogInformation("NetSuite: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── SuiteQL query builder ────────────────────────────────────────────

        private static string BuildSuiteQlQuery(
            string resource, string? customQuery,
            string? watermarkField, string? watermarkValue, int offset)
        {
            if (!string.IsNullOrEmpty(customQuery))
            {
                // Inject OFFSET/FETCH into custom query if not already present.
                if (!customQuery.Contains("OFFSET", StringComparison.OrdinalIgnoreCase))
                {
                    customQuery = $"{customQuery.TrimEnd().TrimEnd(';')} OFFSET {offset} FETCH NEXT {PageLimit} ROWS ONLY";
                }
                return customQuery;
            }

            var sb = new StringBuilder($"SELECT * FROM {resource}");

            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
            {
                sb.Append($" WHERE {watermarkField} > '{watermarkValue}'");
            }

            sb.Append($" OFFSET {offset} FETCH NEXT {PageLimit} ROWS ONLY");

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("items", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard NetSuite SuiteQL response shape
            }
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
                    row[prop.Name] = ConvertJsonValue(prop.Value);
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
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        private string ResolveAccessToken(DataSourceConfig config)
        {
            var token = GetStringParam(config.Parameters, "accessToken");
            if (!string.IsNullOrEmpty(token)) return token;

            // Try connectionString JSON.
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    if (doc.RootElement.TryGetProperty("accessToken", out var t))
                        return t.GetString()
                            ?? throw new ConnectorException(
                                "NetSuite access token is required.",
                                new ArgumentException("Missing 'accessToken'."),
                                "netsuite");
                }
                catch (JsonException)
                {
                    // ConnectionString is not JSON — treat as Bearer token.
                    return config.ConnectionString;
                }
            }

            throw new ConnectorException(
                "NetSuite access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                new ArgumentException("Missing 'accessToken'."),
                "netsuite");
        }

        private string ResolveAccountId(DataSourceConfig config)
        {
            var accountId = GetStringParam(config.Parameters, "accountId");
            if (!string.IsNullOrEmpty(accountId)) return accountId;

            // Try connectionString JSON.
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    if (doc.RootElement.TryGetProperty("accountId", out var a))
                        return a.GetString()
                            ?? throw new ConnectorException(
                                "NetSuite 'accountId' is required.",
                                new ArgumentException("Missing 'accountId'."),
                                "netsuite");
                }
                catch (JsonException) { /* not JSON */ }
            }

            throw new ConnectorException(
                "NetSuite 'accountId' parameter is required.",
                new ArgumentException("Missing 'accountId'."),
                "netsuite");
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
                    $"NetSuite connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "netsuite");
            return value;
        }
    }
}
