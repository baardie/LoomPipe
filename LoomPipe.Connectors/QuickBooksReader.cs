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
    /// Reads accounting data from QuickBooks Online using the v3 API.
    ///
    /// Parameters:
    ///   accessToken  — OAuth 2.0 access token
    ///   realmId      — QuickBooks company ID (realm)
    ///   resource     — Invoice, Customer, Vendor, Item, Account, Payment, Bill, Estimate,
    ///                  PurchaseOrder, Employee, Department, Class, TaxCode, JournalEntry,
    ///                  CreditMemo, SalesReceipt, Transfer
    ///   query        — optional custom QQL query (overrides default SELECT * FROM {resource})
    ///   useSandbox   — optional bool; when "true", uses sandbox-quickbooks.api.intuit.com
    /// </summary>
    public class QuickBooksReader : ISourceReader
    {
        private const string ProductionBaseUrl = "https://quickbooks.api.intuit.com/v3";
        private const string SandboxBaseUrl = "https://sandbox-quickbooks.api.intuit.com/v3";
        private const int MaxResults = 100;

        private static readonly string[] AllResources =
        {
            "Invoice", "Customer", "Vendor", "Item", "Account", "Payment",
            "Bill", "Estimate", "PurchaseOrder", "Employee", "Department",
            "Class", "TaxCode", "JournalEntry", "CreditMemo", "SalesReceipt",
            "Transfer"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<QuickBooksReader> _logger;

        public QuickBooksReader(HttpClient httpClient, ILogger<QuickBooksReader> logger)
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
                    "QuickBooks access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "quickbooks");
            var realmId     = GetRequiredParam(config.Parameters, "realmId");
            var customQuery = GetStringParam(config.Parameters, "query");
            var useSandbox  = string.Equals(GetStringParam(config.Parameters, "useSandbox"), "true", StringComparison.OrdinalIgnoreCase);

            _logger.LogInformation("QuickBooks: reading resource '{Resource}' from realm '{RealmId}'.", resource, realmId);

            try
            {
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    return await ReadWithQueryAsync(
                        realmId, accessToken, useSandbox,
                        $"SELECT * FROM {resource} WHERE {watermarkField} > '{watermarkValue}' STARTPOSITION 1 MAXRESULTS {MaxResults}",
                        resource);
                }

                if (!string.IsNullOrEmpty(customQuery))
                {
                    return await ReadWithQueryAsync(realmId, accessToken, useSandbox, customQuery, resource);
                }

                return await ReadFullAsync(realmId, accessToken, useSandbox, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "QuickBooks: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read QuickBooks resource '{resource}': {ex.Message}", ex, "quickbooks");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("QuickBooks: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await DryRunPreviewAsync(config, 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "QuickBooks: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover QuickBooks schema for '{resource}': {ex.Message}", ex, "quickbooks");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "QuickBooks access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "quickbooks");
            var realmId    = GetRequiredParam(config.Parameters, "realmId");
            var useSandbox = string.Equals(GetStringParam(config.Parameters, "useSandbox"), "true", StringComparison.OrdinalIgnoreCase);

            _logger.LogInformation("QuickBooks: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var query = $"SELECT * FROM {resource} STARTPOSITION 1 MAXRESULTS {sampleSize}";
                var records = await ReadWithQueryAsync(realmId, accessToken, useSandbox, query, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "QuickBooks: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"QuickBooks dry run preview failed for '{resource}': {ex.Message}", ex, "quickbooks");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated QQL) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string realmId, string accessToken, bool useSandbox, string resource, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 1;
            int page = 0;

            do
            {
                var query = $"SELECT * FROM {resource} STARTPOSITION {offset} MAXRESULTS {MaxResults}";
                var url = BuildQueryUrl(realmId, useSandbox, query);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var count = ParseQueryResponse(doc.RootElement, resource, results);

                page++;

                // If we got fewer than MaxResults, we've reached the end.
                if (count < MaxResults || page >= maxPages)
                    break;

                offset += MaxResults;
            }
            while (true);

            _logger.LogInformation("QuickBooks: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Query-based read (paginated) ─────────────────────────────────────

        private async Task<List<object>> ReadWithQueryAsync(
            string realmId, string accessToken, bool useSandbox,
            string baseQuery, string resource, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 1;
            int page = 0;

            // Strip any existing STARTPOSITION/MAXRESULTS from the user query so we control pagination.
            var cleanQuery = System.Text.RegularExpressions.Regex.Replace(
                baseQuery, @"\s+(STARTPOSITION|MAXRESULTS)\s+\d+", "", System.Text.RegularExpressions.RegexOptions.IgnoreCase).Trim();

            do
            {
                var paginatedQuery = $"{cleanQuery} STARTPOSITION {offset} MAXRESULTS {MaxResults}";
                var url = BuildQueryUrl(realmId, useSandbox, paginatedQuery);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var count = ParseQueryResponse(doc.RootElement, resource, results);

                page++;

                if (count < MaxResults || page >= maxPages)
                    break;

                offset += MaxResults;
            }
            while (true);

            _logger.LogInformation("QuickBooks: query returned {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildQueryUrl(string realmId, bool useSandbox, string query)
        {
            var baseUrl = useSandbox ? SandboxBaseUrl : ProductionBaseUrl;
            return $"{baseUrl}/company/{realmId}/query?query={Uri.EscapeDataString(query)}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses the QuickBooks query response and returns the count of records found on this page.
        /// Response shape: { "QueryResponse": { "{Resource}": [...], "startPosition": N, "maxResults": N, "totalCount": N } }
        /// </summary>
        private static int ParseQueryResponse(JsonElement root, string resource, List<object> results)
        {
            if (!root.TryGetProperty("QueryResponse", out var queryResponse))
                return 0;

            // QuickBooks uses the PascalCase resource name as the key.
            if (!queryResponse.TryGetProperty(resource, out var items) || items.ValueKind != JsonValueKind.Array)
                return 0;

            int count = 0;
            foreach (var element in items.EnumerateArray())
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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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
                    $"QuickBooks connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "quickbooks");
            return value;
        }
    }
}
