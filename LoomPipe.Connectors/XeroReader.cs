#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads accounting data from the Xero Accounting API (v2.0).
    ///
    /// Parameters:
    ///   accessToken    — OAuth 2.0 access token
    ///   tenantId       — Xero tenant ID (Xero-Tenant-Id header)
    ///   resource       — Invoices, Contacts, Accounts, BankTransactions, Payments,
    ///                    CreditNotes, PurchaseOrders, ManualJournals, Items, Employees,
    ///                    TrackingCategories, Currencies, TaxRates, Organisation
    ///   modifiedAfter  — optional ISO date string; sets If-Modified-Since header for incremental loads
    /// </summary>
    public class XeroReader : ISourceReader
    {
        private const string BaseUrl = "https://api.xero.com/api.xro/2.0";

        private static readonly string[] AllResources =
        {
            "Invoices", "Contacts", "Accounts", "BankTransactions", "Payments",
            "CreditNotes", "PurchaseOrders", "ManualJournals", "Items",
            "Employees", "TrackingCategories", "Currencies", "TaxRates",
            "Organisation"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<XeroReader> _logger;

        public XeroReader(HttpClient httpClient, ILogger<XeroReader> logger)
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
                    "Xero access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "xero");
            var tenantId       = GetRequiredParam(config.Parameters, "tenantId");
            var modifiedAfter  = GetStringParam(config.Parameters, "modifiedAfter");

            _logger.LogInformation("Xero: reading resource '{Resource}' for tenant '{TenantId}'.", resource, tenantId);

            try
            {
                // Watermark-based incremental: use the watermark value as If-Modified-Since.
                string? ifModifiedSince = null;
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    ifModifiedSince = watermarkValue;
                }
                else if (!string.IsNullOrEmpty(modifiedAfter))
                {
                    ifModifiedSince = modifiedAfter;
                }

                return await ReadFullAsync(resource, accessToken, tenantId, ifModifiedSince);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Xero: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Xero resource '{resource}': {ex.Message}", ex, "xero");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Xero: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await DryRunPreviewAsync(config, 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Xero: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Xero schema for '{resource}': {ex.Message}", ex, "xero");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Xero access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "xero");
            var tenantId = GetRequiredParam(config.Parameters, "tenantId");

            _logger.LogInformation("Xero: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                // Fetch only one page and take the sample.
                var records = await ReadFullAsync(resource, accessToken, tenantId, ifModifiedSince: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Xero: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Xero dry run preview failed for '{resource}': {ex.Message}", ex, "xero");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (page-based pagination) ────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string tenantId,
            string? ifModifiedSince, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;
            int pagesRead = 0;

            do
            {
                var url = $"{BaseUrl}/{resource}?page={page}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, tenantId);

                if (!string.IsNullOrEmpty(ifModifiedSince))
                {
                    request.Headers.TryAddWithoutValidation("If-Modified-Since", ifModifiedSince);
                }

                using var response = await _httpClient.SendAsync(request);

                // 304 Not Modified means no changes since the If-Modified-Since date.
                if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
                {
                    _logger.LogInformation("Xero: no changes since '{ModifiedSince}' for '{Resource}'.", ifModifiedSince, resource);
                    break;
                }

                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var count = ParseResourceResponse(doc.RootElement, resource, results);

                pagesRead++;
                page++;

                // If we got an empty array or fewer results, pagination is done.
                if (count == 0 || pagesRead >= maxPages)
                    break;
            }
            while (true);

            _logger.LogInformation("Xero: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pagesRead);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a Xero API response. Response shape: { "{Resource}": [...] }
        /// Returns the number of records found on this page.
        /// </summary>
        private static int ParseResourceResponse(JsonElement root, string resource, List<object> results)
        {
            // Xero wraps the array under the resource name (plural).
            if (!root.TryGetProperty(resource, out var items) || items.ValueKind != JsonValueKind.Array)
            {
                // Some endpoints may use a slightly different key; try to find any array.
                foreach (var prop in root.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        items = prop.Value;
                        break;
                    }
                }

                if (items.ValueKind != JsonValueKind.Array)
                    return 0;
            }

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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string tenantId)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.TryAddWithoutValidation("Xero-Tenant-Id", tenantId);
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
                    $"Xero connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "xero");
            return value;
        }
    }
}
