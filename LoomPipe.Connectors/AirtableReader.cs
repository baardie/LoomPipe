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
    /// Reads records from Airtable using the REST API.
    ///
    /// Parameters:
    ///   accessToken  — Airtable Personal Access Token or API key
    ///   baseId       — Airtable base ID (e.g. "appXXXXXXXXXXXXXX")
    ///   tableName    — table name or ID to read from
    /// </summary>
    public class AirtableReader : ISourceReader
    {
        private const string BaseUrl = "https://api.airtable.com/v0";
        private const int PageLimit = 100;

        private readonly HttpClient _httpClient;
        private readonly ILogger<AirtableReader> _logger;

        public AirtableReader(HttpClient httpClient, ILogger<AirtableReader> logger)
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
            var (baseId, tableName, accessToken) = ResolveParams(config);

            _logger.LogInformation("Airtable: reading table '{Table}' from base '{Base}'.", tableName, baseId);

            try
            {
                var records = await ReadAllRecordsAsync(baseId, tableName, accessToken);

                // Apply watermark filter client-side if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records.Where(r =>
                    {
                        if (r is IDictionary<string, object> dict && dict.TryGetValue(watermarkField, out var val))
                        {
                            return string.Compare(val?.ToString() ?? "", watermarkValue, StringComparison.Ordinal) > 0;
                        }
                        return false;
                    }).ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: failed to read table '{Table}'.", tableName);
                throw new ConnectorException($"Failed to read Airtable table '{tableName}': {ex.Message}", ex, "airtable");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var (baseId, tableName, accessToken) = ResolveParams(config);

            _logger.LogInformation("Airtable: discovering schema for table '{Table}'.", tableName);

            try
            {
                // Try the metadata API first to get field definitions.
                var fields = await DiscoverSchemaViaMetadataAsync(baseId, tableName, accessToken);
                if (fields.Count > 0) return fields;

                // Fallback: read a sample record and inspect its keys.
                var records = await ReadAllRecordsAsync(baseId, tableName, accessToken, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: failed to discover schema for '{Table}'.", tableName);
                throw new ConnectorException($"Failed to discover Airtable schema for '{tableName}': {ex.Message}", ex, "airtable");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (baseId, tableName, accessToken) = ResolveParams(config);

            _logger.LogInformation("Airtable: dry run preview for '{Table}' (sample={SampleSize}).", tableName, sampleSize);

            try
            {
                var records = await ReadAllRecordsAsync(baseId, tableName, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: dry run preview failed for '{Table}'.", tableName);
                throw new ConnectorException($"Airtable dry run preview failed for '{tableName}': {ex.Message}", ex, "airtable");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Airtable access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "airtable");
            var baseId = GetRequiredParam(config.Parameters, "baseId");

            _logger.LogInformation("Airtable: listing tables for base '{Base}'.", baseId);

            try
            {
                var url = $"https://api.airtable.com/v0/meta/bases/{Uri.EscapeDataString(baseId)}/tables";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var tables = new List<string>();
                if (doc.RootElement.TryGetProperty("tables", out var tablesArr)
                    && tablesArr.ValueKind == JsonValueKind.Array)
                {
                    foreach (var table in tablesArr.EnumerateArray())
                    {
                        if (table.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                        {
                            tables.Add(name.GetString()!);
                        }
                    }
                }

                return tables;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: failed to list tables for base '{Base}'.", baseId);
                throw new ConnectorException($"Failed to list Airtable tables for base '{baseId}': {ex.Message}", ex, "airtable");
            }
        }

        // ── Paginated record read ────────────────────────────────────────────

        private async Task<List<object>> ReadAllRecordsAsync(
            string baseId, string tableName, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? offset = null;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/{Uri.EscapeDataString(baseId)}/{Uri.EscapeDataString(tableName)}?pageSize={PageLimit}";
                if (!string.IsNullOrEmpty(offset))
                    url += $"&offset={Uri.EscapeDataString(offset)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("records", out var records) && records.ValueKind == JsonValueKind.Array)
                {
                    foreach (var record in records.EnumerateArray())
                    {
                        results.Add(FlattenRecord(record));
                    }
                }

                // Pagination: offset token in response.
                offset = null;
                if (root.TryGetProperty("offset", out var offsetEl) && offsetEl.ValueKind == JsonValueKind.String)
                {
                    offset = offsetEl.GetString();
                }

                page++;
            }
            while (offset != null && page < maxPages);

            _logger.LogInformation("Airtable: read {Count} records from '{Table}' across {Pages} page(s).",
                results.Count, tableName, page);

            return results;
        }

        // ── Schema discovery via metadata API ────────────────────────────────

        private async Task<List<string>> DiscoverSchemaViaMetadataAsync(
            string baseId, string tableName, string accessToken)
        {
            try
            {
                // First get the table ID from the base metadata.
                var tablesUrl = $"https://api.airtable.com/v0/meta/bases/{Uri.EscapeDataString(baseId)}/tables";

                using var tablesRequest = new HttpRequestMessage(HttpMethod.Get, tablesUrl);
                ApplyAuth(tablesRequest, accessToken);

                using var tablesResponse = await _httpClient.SendAsync(tablesRequest);
                tablesResponse.EnsureSuccessStatusCode();

                var tablesJson = await tablesResponse.Content.ReadAsStringAsync();
                using var tablesDoc = JsonDocument.Parse(tablesJson);

                var fieldNames = new List<string> { "id", "createdTime" };

                if (tablesDoc.RootElement.TryGetProperty("tables", out var tables)
                    && tables.ValueKind == JsonValueKind.Array)
                {
                    foreach (var table in tables.EnumerateArray())
                    {
                        var name = table.TryGetProperty("name", out var n) ? n.GetString() : null;
                        if (string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
                        {
                            if (table.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Array)
                            {
                                foreach (var field in fields.EnumerateArray())
                                {
                                    if (field.TryGetProperty("name", out var fieldName) && fieldName.ValueKind == JsonValueKind.String)
                                    {
                                        fieldNames.Add(fieldName.GetString()!);
                                    }
                                }
                            }
                            break;
                        }
                    }
                }

                return fieldNames;
            }
            catch
            {
                // Metadata API may not be available — return empty to trigger fallback.
                return new List<string>();
            }
        }

        // ── Record flattening ────────────────────────────────────────────────

        /// <summary>
        /// Flattens an Airtable record into an ExpandoObject.
        /// Each record has id, createdTime, and a fields object whose properties are spread into the row.
        /// </summary>
        private static object FlattenRecord(JsonElement record)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (record.TryGetProperty("id", out var id))
                row["id"] = id.GetString() ?? id.ToString();

            if (record.TryGetProperty("createdTime", out var createdTime))
                row["createdTime"] = createdTime.GetString() ?? createdTime.ToString();

            // Spread all fields into the top-level row.
            if (record.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in fields.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }
            }

            return row;
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

        // ── Parameter resolution ─────────────────────────────────────────────

        private (string baseId, string tableName, string accessToken) ResolveParams(DataSourceConfig config)
        {
            var baseId      = GetRequiredParam(config.Parameters, "baseId");
            var tableName   = GetRequiredParam(config.Parameters, "tableName");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Airtable access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "airtable");

            return (baseId, tableName, accessToken);
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
                    $"Airtable connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "airtable");
            return value;
        }
    }
}
