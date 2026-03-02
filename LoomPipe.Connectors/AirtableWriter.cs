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
    /// Writes records to Airtable using the REST API.
    /// Batches up to 10 records per request (Airtable API limit).
    ///
    /// Parameters:
    ///   accessToken  — Airtable Personal Access Token or API key
    ///   baseId       — Airtable base ID (e.g. "appXXXXXXXXXXXXXX")
    ///   tableName    — table name or ID to write to
    /// </summary>
    public class AirtableWriter : IDestinationWriter
    {
        private const string BaseUrl = "https://api.airtable.com/v0";
        private const int BatchSize = 10; // Airtable API limit

        private readonly HttpClient _httpClient;
        private readonly ILogger<AirtableWriter> _logger;

        public AirtableWriter(HttpClient httpClient, ILogger<AirtableWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── IDestinationWriter ───────────────────────────────────────────────

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var (baseId, tableName, accessToken) = ResolveParams(config);

            _logger.LogInformation("Airtable: writing to table '{Table}' in base '{Base}'.", tableName, baseId);

            try
            {
                var url = $"{BaseUrl}/{Uri.EscapeDataString(baseId)}/{Uri.EscapeDataString(tableName)}";
                var recordList = records.ToList();
                var totalWritten = 0;

                // Batch records in groups of 10 (Airtable API limit).
                for (int i = 0; i < recordList.Count; i += BatchSize)
                {
                    var batch = recordList.Skip(i).Take(BatchSize).ToList();
                    var body = BuildCreateBody(batch);

                    using var request = new HttpRequestMessage(HttpMethod.Post, url);
                    ApplyAuth(request, accessToken);
                    request.Content = new StringContent(body, Encoding.UTF8, "application/json");

                    using var response = await _httpClient.SendAsync(request);

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorBody = await response.Content.ReadAsStringAsync();
                        throw new ConnectorException(
                            $"Airtable API returned {(int)response.StatusCode}: {errorBody}",
                            new HttpRequestException($"HTTP {(int)response.StatusCode}"),
                            "airtable");
                    }

                    totalWritten += batch.Count;
                }

                _logger.LogInformation(
                    "Airtable: successfully wrote {Count} records to '{Table}' in {Batches} batch(es).",
                    totalWritten, tableName, (int)Math.Ceiling((double)recordList.Count / BatchSize));
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: failed to write to table '{Table}'.", tableName);
                throw new ConnectorException($"Failed to write to Airtable table '{tableName}': {ex.Message}", ex, "airtable");
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            var (baseId, tableName, accessToken) = ResolveParams(config);

            _logger.LogInformation("Airtable: validating schema for table '{Table}'.", tableName);

            try
            {
                // Fetch table metadata to get field definitions.
                var tablesUrl = $"https://api.airtable.com/v0/meta/bases/{Uri.EscapeDataString(baseId)}/tables";

                using var request = new HttpRequestMessage(HttpMethod.Get, tablesUrl);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var tableFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (doc.RootElement.TryGetProperty("tables", out var tables)
                    && tables.ValueKind == JsonValueKind.Array)
                {
                    foreach (var table in tables.EnumerateArray())
                    {
                        var name = table.TryGetProperty("name", out var n) ? n.GetString() : null;
                        if (string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
                        {
                            if (table.TryGetProperty("fields", out var fieldsArr)
                                && fieldsArr.ValueKind == JsonValueKind.Array)
                            {
                                foreach (var field in fieldsArr.EnumerateArray())
                                {
                                    if (field.TryGetProperty("name", out var fieldName)
                                        && fieldName.ValueKind == JsonValueKind.String)
                                    {
                                        tableFields.Add(fieldName.GetString()!);
                                    }
                                }
                            }
                            break;
                        }
                    }
                }

                if (tableFields.Count == 0)
                {
                    _logger.LogWarning("Airtable: could not retrieve field metadata for '{Table}'. Returning true.", tableName);
                    return true;
                }

                // Check that all requested fields exist in the table.
                var missingFields = fields.Where(f =>
                    !string.Equals(f, "id", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(f, "createdTime", StringComparison.OrdinalIgnoreCase)
                    && !tableFields.Contains(f)).ToList();

                if (missingFields.Count > 0)
                {
                    _logger.LogWarning(
                        "Airtable: fields not found in table '{Table}': {Fields}",
                        tableName, string.Join(", ", missingFields));
                    return false;
                }

                return true;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Airtable: schema validation failed for '{Table}'.", tableName);
                throw new ConnectorException($"Airtable schema validation failed for '{tableName}': {ex.Message}", ex, "airtable");
            }
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            var baseId    = GetStringParam(config.Parameters, "baseId") ?? "unknown";
            var tableName = GetStringParam(config.Parameters, "tableName") ?? "unknown";

            _logger.LogInformation(
                "Airtable: dry run preview for writer (table={Table}, sampleSize={SampleSize}).",
                tableName, sampleSize);

            var preview = records.Take(sampleSize).Select(record =>
            {
                IDictionary<string, object> expando = new ExpandoObject();
                expando["_airtable_endpoint"] = $"POST /v0/{baseId}/{tableName}";
                expando["_airtable_batch_size"] = BatchSize;
                expando["_airtable_payload"] = record;
                return (object)expando;
            });

            return Task.FromResult(preview);
        }

        // ── Request body builder ─────────────────────────────────────────────

        private static string BuildCreateBody(List<object> batch)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WritePropertyName("records");
            writer.WriteStartArray();

            foreach (var record in batch)
            {
                writer.WriteStartObject();
                writer.WritePropertyName("fields");
                writer.WriteStartObject();

                if (record is IDictionary<string, object> dict)
                {
                    foreach (var kvp in dict)
                    {
                        // Skip Airtable system fields — they cannot be written.
                        if (string.Equals(kvp.Key, "id", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(kvp.Key, "createdTime", StringComparison.OrdinalIgnoreCase))
                            continue;

                        writer.WritePropertyName(kvp.Key);
                        WriteJsonValue(writer, kvp.Value);
                    }
                }

                writer.WriteEndObject(); // fields
                writer.WriteEndObject(); // record
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static void WriteJsonValue(Utf8JsonWriter writer, object? value)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    break;
                case string s:
                    writer.WriteStringValue(s);
                    break;
                case int i:
                    writer.WriteNumberValue(i);
                    break;
                case long l:
                    writer.WriteNumberValue(l);
                    break;
                case double d:
                    writer.WriteNumberValue(d);
                    break;
                case decimal dec:
                    writer.WriteNumberValue(dec);
                    break;
                case bool b:
                    writer.WriteBooleanValue(b);
                    break;
                case JsonElement je:
                    je.WriteTo(writer);
                    break;
                default:
                    writer.WriteStringValue(value.ToString());
                    break;
            }
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
