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
    /// Reads records from Zoho CRM using the v7 API.
    ///
    /// Parameters:
    ///   accessToken    — Zoho CRM OAuth access token (Bearer)
    ///   resource       — Leads, Contacts, Accounts, Deals, Tasks, Events, Calls, Products,
    ///                   Quotes, Invoices, Purchase_Orders, Sales_Orders, Campaigns, Vendors,
    ///                   Solutions, Cases
    ///   modifiedAfter  — optional ISO date for incremental filtering (If-Modified-Since header)
    /// </summary>
    public class ZohoCrmReader : ISourceReader
    {
        private const string BaseUrl = "https://www.zohoapis.com/crm/v7";
        private const int PageLimit = 200;

        private static readonly string[] AllResources =
        {
            "Leads", "Contacts", "Accounts", "Deals", "Tasks", "Events",
            "Calls", "Products", "Quotes", "Invoices", "Purchase_Orders",
            "Sales_Orders", "Campaigns", "Vendors", "Solutions", "Cases"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ZohoCrmReader> _logger;

        public ZohoCrmReader(HttpClient httpClient, ILogger<ZohoCrmReader> logger)
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
            var accessToken = ResolveAccessToken(config);
            var modifiedAfter = GetStringParam(config.Parameters, "modifiedAfter");

            _logger.LogInformation("ZohoCRM: reading resource '{Resource}'.", resource);

            try
            {
                // Prefer watermark over modifiedAfter parameter.
                string? ifModifiedSince = null;
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    ifModifiedSince = watermarkValue;
                }
                else if (!string.IsNullOrEmpty(modifiedAfter))
                {
                    ifModifiedSince = modifiedAfter;
                }

                return await ReadPaginatedAsync(resource, accessToken, ifModifiedSince);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ZohoCRM: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Zoho CRM resource '{resource}': {ex.Message}", ex, "zohocrm");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = ResolveAccessToken(config);

            _logger.LogInformation("ZohoCRM: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadPaginatedAsync(resource, accessToken, ifModifiedSince: null, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ZohoCRM: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Zoho CRM schema for '{resource}': {ex.Message}", ex, "zohocrm");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = ResolveAccessToken(config);

            _logger.LogInformation("ZohoCRM: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(resource, accessToken, ifModifiedSince: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ZohoCRM: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Zoho CRM dry run preview failed for '{resource}': {ex.Message}", ex, "zohocrm");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string resource, string accessToken, string? ifModifiedSince,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;
            int pageCount = 0;
            bool moreRecords = true;

            while (moreRecords && pageCount < maxPages)
            {
                var url = $"{BaseUrl}/{resource}?per_page={PageLimit}&page={page}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                if (!string.IsNullOrEmpty(ifModifiedSince))
                {
                    request.Headers.TryAddWithoutValidation("If-Modified-Since", ifModifiedSince);
                }

                using var response = await _httpClient.SendAsync(request);

                // 304 Not Modified means no new data.
                if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
                    break;

                // 204 No Content means empty module.
                if (response.StatusCode == System.Net.HttpStatusCode.NoContent)
                    break;

                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, results);

                // Check "info.more_records" for pagination.
                moreRecords = false;
                if (doc.RootElement.TryGetProperty("info", out var info)
                    && info.TryGetProperty("more_records", out var more)
                    && more.ValueKind == JsonValueKind.True)
                {
                    moreRecords = true;
                }

                pageCount++;
                page++;
            }

            _logger.LogInformation("ZohoCRM: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pageCount);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Zoho CRM response shape
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
            return GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Zoho CRM access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "zohocrm");
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
                    $"Zoho CRM connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "zohocrm");
            return value;
        }
    }
}
