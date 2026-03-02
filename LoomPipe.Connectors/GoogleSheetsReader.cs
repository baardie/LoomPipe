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
    /// Google Sheets source reader.  Uses the Sheets API v4 over plain HttpClient
    /// (no external SDK).  Supports public sheets via API key and private sheets
    /// via OAuth access token.
    /// </summary>
    public class GoogleSheetsReader : ISourceReader
    {
        private const string SheetsApiBase = "https://sheets.googleapis.com/v4/spreadsheets";

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleSheetsReader> _logger;

        public GoogleSheetsReader(HttpClient httpClient, ILogger<GoogleSheetsReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ────────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var parameters = MergeConnectionString(config);
            var spreadsheetId = GetRequiredParam(parameters, "spreadsheetId");
            _logger.LogInformation("Reading Google Sheet '{SpreadsheetId}'.", spreadsheetId);

            try
            {
                var values = await FetchValuesAsync(parameters, spreadsheetId);
                if (values.Count == 0)
                    return Array.Empty<object>();

                var useHeaders = GetHeaderRowFlag(parameters);
                var (headers, dataStartIndex) = BuildHeaders(values, useHeaders);
                var records = ConvertToRecords(values, headers, dataStartIndex);

                // Watermark filtering (simple string comparison)
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records
                        .Where(r =>
                        {
                            var dict = (IDictionary<string, object>)r;
                            if (!dict.TryGetValue(watermarkField, out var val)) return false;
                            return string.Compare(val?.ToString() ?? "", watermarkValue, StringComparison.Ordinal) > 0;
                        })
                        .ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Google Sheet '{SpreadsheetId}'.", spreadsheetId);
                throw new ConnectorException($"Failed to read from Google Sheet: {ex.Message}", ex, "googlesheets");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var spreadsheetId = GetRequiredParam(parameters, "spreadsheetId");
            _logger.LogInformation("Discovering schema for Google Sheet '{SpreadsheetId}'.", spreadsheetId);

            try
            {
                var values = await FetchValuesAsync(parameters, spreadsheetId);
                if (values.Count == 0)
                    return Array.Empty<string>();

                var useHeaders = GetHeaderRowFlag(parameters);
                var (headers, _) = BuildHeaders(values, useHeaders);
                return headers;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Google Sheet '{SpreadsheetId}'.", spreadsheetId);
                throw new ConnectorException($"Failed to discover Google Sheets schema: {ex.Message}", ex, "googlesheets");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for Google Sheet.");
            var records = await ReadAsync(config);
            return records.Take(sampleSize);
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var spreadsheetId = GetRequiredParam(parameters, "spreadsheetId");
            _logger.LogInformation("Listing sheet tabs for Google Sheet '{SpreadsheetId}'.", spreadsheetId);

            try
            {
                var url = $"{SheetsApiBase}/{Uri.EscapeDataString(spreadsheetId)}?fields=sheets.properties.title";
                url = AppendAuth(url, parameters);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyBearerToken(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var sheetNames = new List<string>();
                if (doc.RootElement.TryGetProperty("sheets", out var sheetsArray))
                {
                    foreach (var sheet in sheetsArray.EnumerateArray())
                    {
                        if (sheet.TryGetProperty("properties", out var props) &&
                            props.TryGetProperty("title", out var title))
                        {
                            sheetNames.Add(title.GetString() ?? "");
                        }
                    }
                }

                return sheetNames;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list resources for Google Sheet '{SpreadsheetId}'.", spreadsheetId);
                throw new ConnectorException($"Failed to list Google Sheet tabs: {ex.Message}", ex, "googlesheets");
            }
        }

        // ── Private helpers ──────────────────────────────────────────────────────

        /// <summary>
        /// Fetches the raw 2-D values grid from the Sheets API.
        /// </summary>
        private async Task<List<List<string>>> FetchValuesAsync(
            Dictionary<string, object> parameters, string spreadsheetId)
        {
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

            var rows = new List<List<string>>();
            if (doc.RootElement.TryGetProperty("values", out var valuesArray))
            {
                foreach (var row in valuesArray.EnumerateArray())
                {
                    var cells = new List<string>();
                    foreach (var cell in row.EnumerateArray())
                    {
                        cells.Add(cell.GetString() ?? "");
                    }
                    rows.Add(cells);
                }
            }

            return rows;
        }

        /// <summary>
        /// Builds the A1-notation range string from parameters.
        /// Priority: explicit range > sheetName > empty (reads all).
        /// </summary>
        private static string BuildRange(Dictionary<string, object> parameters)
        {
            var range = GetStringParam(parameters, "range");
            if (!string.IsNullOrWhiteSpace(range))
                return range;

            var sheetName = GetStringParam(parameters, "sheetName");
            if (!string.IsNullOrWhiteSpace(sheetName))
                return sheetName;

            // Default: read the entire first sheet
            return "Sheet1";
        }

        /// <summary>
        /// Returns true when the first row should be treated as column headers (default behaviour).
        /// </summary>
        private static bool GetHeaderRowFlag(Dictionary<string, object> parameters)
        {
            var headerRow = GetStringParam(parameters, "headerRow");
            if (string.IsNullOrWhiteSpace(headerRow))
                return true; // default
            return !string.Equals(headerRow, "false", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Extracts column headers and the index of the first data row.
        /// </summary>
        private static (List<string> Headers, int DataStartIndex) BuildHeaders(
            List<List<string>> values, bool useHeaders)
        {
            if (useHeaders && values.Count > 0)
            {
                var headers = values[0].Select(h => string.IsNullOrWhiteSpace(h) ? $"Col_{values[0].IndexOf(h)}" : h).ToList();
                // Deduplicate: if headers has repeated names, append suffix
                var seen = new Dictionary<string, int>();
                for (var i = 0; i < headers.Count; i++)
                {
                    var original = headers[i];
                    if (seen.ContainsKey(original))
                    {
                        seen[original]++;
                        headers[i] = $"{original}_{seen[original]}";
                    }
                    else
                    {
                        seen[original] = 0;
                    }
                }
                return (headers, 1);
            }

            // No header row — generate Col_0, Col_1, ...
            var maxCols = values.Max(r => r.Count);
            var generated = Enumerable.Range(0, maxCols).Select(i => $"Col_{i}").ToList();
            return (generated, 0);
        }

        /// <summary>
        /// Converts raw row data into a list of ExpandoObjects.
        /// </summary>
        private static List<object> ConvertToRecords(
            List<List<string>> values, List<string> headers, int dataStartIndex)
        {
            var records = new List<object>();
            for (var i = dataStartIndex; i < values.Count; i++)
            {
                IDictionary<string, object> expando = new ExpandoObject();
                for (var j = 0; j < headers.Count; j++)
                {
                    expando[headers[j]] = j < values[i].Count ? values[i][j] : "";
                }
                records.Add(expando);
            }
            return records;
        }

        /// <summary>
        /// Appends API key as a query-string parameter when no access token is present.
        /// </summary>
        private static string AppendAuth(string url, Dictionary<string, object> parameters)
        {
            var accessToken = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(accessToken))
                return url; // auth header will be applied separately

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
