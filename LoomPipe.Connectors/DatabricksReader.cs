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
    /// Reads rows from Databricks SQL via the Statement Execution REST API.
    /// POST https://{host}/api/2.0/sql/statements/
    /// ConnectionString JSON: {"accessToken":"...","host":"...","warehouseId":"..."}
    /// Parameters: accessToken, host (workspace URL), warehouseId, catalog, schema, table, query (optional SQL override).
    /// </summary>
    public class DatabricksReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<DatabricksReader> _logger;

        public DatabricksReader(HttpClient httpClient, ILogger<DatabricksReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var parameters = MergeConnectionString(config);
            var table = GetFullyQualifiedTable(parameters);
            _logger.LogInformation("Reading from Databricks table '{Table}'.", table);

            try
            {
                var customQuery = GetStringParam(parameters, "query");
                var sql = !string.IsNullOrWhiteSpace(customQuery)
                    ? customQuery
                    : $"SELECT * FROM {table}";

                if (!string.IsNullOrWhiteSpace(watermarkField) && !string.IsNullOrWhiteSpace(watermarkValue))
                {
                    sql = !string.IsNullOrWhiteSpace(customQuery)
                        ? $"SELECT * FROM ({customQuery}) _q WHERE {watermarkField} > '{watermarkValue}'"
                        : $"SELECT * FROM {table} WHERE {watermarkField} > '{watermarkValue}'";
                }

                return await ExecuteStatementAsync(parameters, sql);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Databricks table '{Table}'.", table);
                throw new ConnectorException($"Failed to read from Databricks: {ex.Message}", ex, "databricks");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var table = GetFullyQualifiedTable(parameters);
            _logger.LogInformation("Discovering schema for Databricks table '{Table}'.", table);

            try
            {
                var result = await ExecuteStatementAsync(parameters, $"SELECT * FROM {table} LIMIT 1");

                var first = result.FirstOrDefault();
                if (first is IDictionary<string, object> dict)
                    return dict.Keys;

                return Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Databricks table '{Table}'.", table);
                throw new ConnectorException($"Failed to discover Databricks schema: {ex.Message}", ex, "databricks");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var table = GetFullyQualifiedTable(parameters);
            _logger.LogInformation("Dry run preview from Databricks table '{Table}'.", table);

            try
            {
                return await ExecuteStatementAsync(parameters, $"SELECT * FROM {table} LIMIT {sampleSize}");
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for Databricks table '{Table}'.", table);
                throw new ConnectorException($"Databricks dry run failed: {ex.Message}", ex, "databricks");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var catalog = GetStringParam(parameters, "catalog");
            var schema = GetStringParam(parameters, "schema");
            _logger.LogInformation("Listing tables in Databricks catalog '{Catalog}', schema '{Schema}'.", catalog, schema);

            try
            {
                string sql;
                if (!string.IsNullOrWhiteSpace(catalog) && !string.IsNullOrWhiteSpace(schema))
                    sql = $"SHOW TABLES IN {catalog}.{schema}";
                else if (!string.IsNullOrWhiteSpace(schema))
                    sql = $"SHOW TABLES IN {schema}";
                else
                    sql = "SHOW TABLES";

                var rows = await ExecuteStatementAsync(parameters, sql);
                return rows.Select(r =>
                {
                    if (r is IDictionary<string, object> dict)
                    {
                        // SHOW TABLES returns tableName column
                        if (dict.TryGetValue("tableName", out var tn)) return tn?.ToString() ?? "";
                        if (dict.TryGetValue("table_name", out var tn2)) return tn2?.ToString() ?? "";
                        // Fallback: return first column value
                        return dict.Values.FirstOrDefault()?.ToString() ?? "";
                    }
                    return r?.ToString() ?? "";
                }).Where(n => !string.IsNullOrEmpty(n)).ToList();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list tables in Databricks.");
                throw new ConnectorException($"Failed to list Databricks tables: {ex.Message}", ex, "databricks");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        /// <summary>
        /// Executes a SQL statement via the Databricks Statement Execution API and returns rows as ExpandoObjects.
        /// </summary>
        private async Task<List<object>> ExecuteStatementAsync(Dictionary<string, object> parameters, string sql)
        {
            var host = GetStringParam(parameters, "host")
                ?? throw new ConnectorException(
                    "Databricks 'host' parameter is required.",
                    new ArgumentException("Missing 'host'."),
                    "databricks");
            var accessToken = GetStringParam(parameters, "accessToken")
                ?? throw new ConnectorException(
                    "Databricks 'accessToken' parameter is required.",
                    new ArgumentException("Missing 'accessToken'."),
                    "databricks");
            var warehouseId = GetStringParam(parameters, "warehouseId")
                ?? throw new ConnectorException(
                    "Databricks 'warehouseId' parameter is required.",
                    new ArgumentException("Missing 'warehouseId'."),
                    "databricks");

            // Normalize host — strip protocol if provided
            var baseUrl = host.StartsWith("http", StringComparison.OrdinalIgnoreCase)
                ? host.TrimEnd('/')
                : $"https://{host.TrimEnd('/')}";

            var requestBody = new
            {
                warehouse_id = warehouseId,
                statement = sql,
                wait_timeout = "50s"
            };

            var jsonContent = new StringContent(
                JsonSerializer.Serialize(requestBody),
                Encoding.UTF8,
                "application/json");

            using var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/api/2.0/sql/statements/");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Content = jsonContent;

            using var response = await _httpClient.SendAsync(request);

            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync();
                throw new ConnectorException(
                    $"Databricks API returned {(int)response.StatusCode}: {TruncateBody(errorBody)}",
                    new HttpRequestException($"HTTP {(int)response.StatusCode}"),
                    "databricks");
            }

            var responseJson = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(responseJson);
            var root = doc.RootElement;

            // Check status
            if (root.TryGetProperty("status", out var status) &&
                status.TryGetProperty("state", out var state))
            {
                var stateStr = state.GetString();
                if (stateStr != "SUCCEEDED")
                {
                    var errorMessage = "";
                    if (status.TryGetProperty("error", out var error) &&
                        error.TryGetProperty("message", out var errMsg))
                        errorMessage = errMsg.GetString() ?? "";

                    throw new ConnectorException(
                        $"Databricks statement failed with state '{stateStr}': {errorMessage}",
                        new InvalidOperationException($"Statement state: {stateStr}"),
                        "databricks");
                }
            }

            // Extract column names from manifest
            var columnNames = new List<string>();
            if (root.TryGetProperty("manifest", out var manifest) &&
                manifest.TryGetProperty("schema", out var schemaObj) &&
                schemaObj.TryGetProperty("columns", out var columns) &&
                columns.ValueKind == JsonValueKind.Array)
            {
                foreach (var col in columns.EnumerateArray())
                {
                    if (col.TryGetProperty("name", out var colName))
                        columnNames.Add(colName.GetString() ?? $"col_{columnNames.Count}");
                }
            }

            // Extract data rows from result.data_array
            var results = new List<object>();
            if (root.TryGetProperty("result", out var result) &&
                result.TryGetProperty("data_array", out var dataArray) &&
                dataArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var row in dataArray.EnumerateArray())
                {
                    if (row.ValueKind != JsonValueKind.Array) continue;

                    IDictionary<string, object> expando = new ExpandoObject();
                    int colIndex = 0;
                    foreach (var cell in row.EnumerateArray())
                    {
                        var colName = colIndex < columnNames.Count ? columnNames[colIndex] : $"col_{colIndex}";
                        expando[colName] = cell.ValueKind switch
                        {
                            JsonValueKind.String => (object)(cell.GetString() ?? string.Empty),
                            JsonValueKind.Number => cell.TryGetInt64(out var l) ? l : cell.GetDouble(),
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.Null => string.Empty,
                            _ => cell.ToString()
                        };
                        colIndex++;
                    }
                    results.Add(expando);
                }
            }

            return results;
        }

        /// <summary>
        /// Builds a fully qualified table name from catalog, schema, and table parameters.
        /// </summary>
        private static string GetFullyQualifiedTable(Dictionary<string, object> parameters)
        {
            var catalog = GetStringParam(parameters, "catalog");
            var schema = GetStringParam(parameters, "schema");
            var table = GetStringParam(parameters, "table") ?? "data";

            if (!string.IsNullOrWhiteSpace(catalog) && !string.IsNullOrWhiteSpace(schema))
                return $"{catalog}.{schema}.{table}";
            if (!string.IsNullOrWhiteSpace(schema))
                return $"{schema}.{table}";
            return table;
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

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string TruncateBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
