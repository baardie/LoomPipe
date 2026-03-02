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
    /// Google Sheets destination writer.  Uses the Sheets API v4 append endpoint
    /// over plain HttpClient (no external SDK).  Supports public sheets via API key
    /// and private sheets via OAuth access token.
    /// </summary>
    public class GoogleSheetsWriter : IDestinationWriter
    {
        private const string SheetsApiBase = "https://sheets.googleapis.com/v4/spreadsheets";

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleSheetsWriter> _logger;

        public GoogleSheetsWriter(HttpClient httpClient, ILogger<GoogleSheetsWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── IDestinationWriter ───────────────────────────────────────────────────

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var parameters = MergeConnectionString(config);
            var spreadsheetId = GetRequiredParam(parameters, "spreadsheetId");
            _logger.LogInformation("Writing to Google Sheet '{SpreadsheetId}'.", spreadsheetId);

            try
            {
                var recordList = records.ToList();
                if (recordList.Count == 0)
                {
                    _logger.LogInformation("No records to write. Skipping.");
                    return;
                }

                // Extract column headers from the first record
                var firstDict = recordList[0] as IDictionary<string, object>
                    ?? throw new InvalidOperationException("Records must be ExpandoObject / IDictionary<string, object>.");

                var headers = firstDict.Keys.ToList();

                // Build the 2-D values array (header row + data rows)
                var rows = new List<List<object?>> { headers.Cast<object?>().ToList() };

                foreach (var record in recordList)
                {
                    var dict = record as IDictionary<string, object>
                        ?? throw new InvalidOperationException("Each record must be ExpandoObject / IDictionary<string, object>.");

                    var row = new List<object?>();
                    foreach (var header in headers)
                    {
                        dict.TryGetValue(header, out var val);
                        row.Add(val?.ToString() ?? "");
                    }
                    rows.Add(row);
                }

                // Build the request URL
                var range = BuildRange(parameters);
                var encodedRange = Uri.EscapeDataString(range);
                var url = $"{SheetsApiBase}/{Uri.EscapeDataString(spreadsheetId)}/values/{encodedRange}:append?valueInputOption=USER_ENTERED";
                url = AppendAuth(url, parameters);

                // Build the JSON body
                var body = new { values = rows };
                var jsonBody = JsonSerializer.Serialize(body);

                using var request = new HttpRequestMessage(HttpMethod.Post, url);
                ApplyBearerToken(request, parameters);
                request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                _logger.LogInformation("Successfully appended {RowCount} rows to Google Sheet '{SpreadsheetId}'.",
                    recordList.Count, spreadsheetId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to Google Sheet '{SpreadsheetId}'.", spreadsheetId);
                throw new ConnectorException($"Failed to write to Google Sheet: {ex.Message}", ex, "googlesheets");
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            var parameters = MergeConnectionString(config);
            var spreadsheetId = GetRequiredParam(parameters, "spreadsheetId");
            _logger.LogInformation("Validating schema for Google Sheet '{SpreadsheetId}'.", spreadsheetId);

            try
            {
                // Fetch the first row to check if the destination has matching headers
                var range = BuildRange(parameters);
                var encodedRange = Uri.EscapeDataString(range);
                var url = $"{SheetsApiBase}/{Uri.EscapeDataString(spreadsheetId)}/values/{encodedRange}";
                url = AppendAuth(url, parameters);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyBearerToken(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                if (!doc.RootElement.TryGetProperty("values", out var valuesArray))
                {
                    // Empty sheet — any schema is valid (headers will be written on first append)
                    return true;
                }

                var headerRow = new List<string>();
                var firstRow = valuesArray.EnumerateArray().FirstOrDefault();
                if (firstRow.ValueKind == JsonValueKind.Array)
                {
                    foreach (var cell in firstRow.EnumerateArray())
                        headerRow.Add(cell.GetString() ?? "");
                }

                if (headerRow.Count == 0)
                    return true; // empty sheet

                // Check that every incoming field exists in the sheet headers
                var fieldList = fields.ToList();
                var missing = fieldList.Except(headerRow, StringComparer.OrdinalIgnoreCase).ToList();
                if (missing.Count > 0)
                {
                    _logger.LogWarning(
                        "Schema validation: the following fields are missing from the sheet headers: {MissingFields}",
                        string.Join(", ", missing));
                    return false;
                }

                return true;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to validate schema for Google Sheet '{SpreadsheetId}'.", spreadsheetId);
                throw new ConnectorException($"Failed to validate Google Sheets schema: {ex.Message}", ex, "googlesheets");
            }
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for Google Sheets writer.");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Private helpers ──────────────────────────────────────────────────────

        /// <summary>
        /// Builds the A1-notation range string from parameters.
        /// </summary>
        private static string BuildRange(Dictionary<string, object> parameters)
        {
            var range = GetStringParam(parameters, "range");
            if (!string.IsNullOrWhiteSpace(range))
                return range;

            var sheetName = GetStringParam(parameters, "sheetName");
            if (!string.IsNullOrWhiteSpace(sheetName))
                return sheetName;

            return "Sheet1";
        }

        /// <summary>
        /// Appends API key as a query-string parameter when no access token is present.
        /// </summary>
        private static string AppendAuth(string url, Dictionary<string, object> parameters)
        {
            var accessToken = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(accessToken))
                return url;

            var apiKey = GetStringParam(parameters, "apiKey");
            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                var separator = url.Contains('?') ? "&" : "?";
                return $"{url}{separator}key={Uri.EscapeDataString(apiKey)}";
            }

            return url;
        }

        /// <summary>
        /// Sets the Authorization: Bearer header when an access token is configured.
        /// </summary>
        private static void ApplyBearerToken(HttpRequestMessage request, Dictionary<string, object> parameters)
        {
            var accessToken = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(accessToken))
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
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
            catch (JsonException) { /* not JSON — ignore */ }
            return merged;
        }

        // ── Parameter helper ─────────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> parameters, string key)
        {
            var value = GetStringParam(parameters, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"Required parameter '{key}' is missing or empty.",
                    new ArgumentException($"Parameter '{key}' is required.", key),
                    "googlesheets");
            return value;
        }
    }
}
