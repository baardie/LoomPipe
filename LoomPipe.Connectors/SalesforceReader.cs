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
    /// Reads sObjects from Salesforce using the REST API (v59.0).
    ///
    /// Parameters:
    ///   accessToken  — Salesforce OAuth access token
    ///   instanceUrl  — Salesforce instance URL (e.g. "https://yourorg.salesforce.com")
    ///   resource     — sObject API name (Account, Contact, Lead, Opportunity, etc.)
    ///   query        — optional SOQL query (overrides default "SELECT ... FROM {resource}")
    /// </summary>
    public class SalesforceReader : ISourceReader
    {
        private const string ApiVersion = "v59.0";
        private const int DefaultPageSize = 2000; // Salesforce default query batch size

        private static readonly string[] AllResources =
        {
            "Account", "Contact", "Lead", "Opportunity", "Case",
            "Task", "Event", "Campaign", "User", "Product2",
            "Order", "Contract", "Note", "ContentDocument",
            "Report", "Dashboard", "CustomObject__c"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SalesforceReader> _logger;

        public SalesforceReader(HttpClient httpClient, ILogger<SalesforceReader> logger)
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
            var parameters  = MergeConnectionString(config);
            var instanceUrl = GetRequiredParam(parameters, "instanceUrl");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var query       = GetStringParam(parameters, "query");

            _logger.LogInformation("Salesforce: reading resource '{Resource}'.", resource);

            try
            {
                string soql;

                if (!string.IsNullOrWhiteSpace(query))
                {
                    soql = query;

                    // Append watermark filter if provided and not already in the query
                    if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                    {
                        var whereClause = $"{watermarkField} > {watermarkValue}";
                        soql = soql.Contains("WHERE", StringComparison.OrdinalIgnoreCase)
                            ? $"{soql} AND {whereClause}"
                            : $"{soql} WHERE {whereClause}";
                    }
                }
                else
                {
                    // Discover fields to build a SELECT * equivalent
                    var fields = await FetchFieldNamesAsync(instanceUrl, accessToken, resource);
                    var fieldList = string.Join(", ", fields);

                    soql = $"SELECT {fieldList} FROM {resource}";

                    if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                    {
                        soql += $" WHERE {watermarkField} > {watermarkValue}";
                    }
                }

                return await ExecuteQueryAsync(instanceUrl, accessToken, soql);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesforce: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Salesforce resource '{resource}': {ex.Message}", ex, "salesforce");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var instanceUrl = GetRequiredParam(parameters, "instanceUrl");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");

            _logger.LogInformation("Salesforce: discovering schema for '{Resource}'.", resource);

            try
            {
                return await FetchFieldNamesAsync(instanceUrl, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesforce: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Salesforce schema for '{resource}': {ex.Message}", ex, "salesforce");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var instanceUrl = GetRequiredParam(parameters, "instanceUrl");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var query       = GetStringParam(parameters, "query");

            _logger.LogInformation("Salesforce: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                string soql;

                if (!string.IsNullOrWhiteSpace(query))
                {
                    // Strip any existing LIMIT and add our own
                    soql = StripLimit(query);
                    soql += $" LIMIT {sampleSize}";
                }
                else
                {
                    var fields = await FetchFieldNamesAsync(instanceUrl, accessToken, resource);
                    var fieldList = string.Join(", ", fields);
                    soql = $"SELECT {fieldList} FROM {resource} LIMIT {sampleSize}";
                }

                return await ExecuteQueryAsync(instanceUrl, accessToken, soql, paginate: false);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesforce: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Salesforce dry run preview failed for '{resource}': {ex.Message}", ex, "salesforce");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var instanceUrl = GetStringParam(parameters, "instanceUrl");
            var accessToken = GetStringParam(parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : null); // handled by MergeConnectionString

            // If we have valid credentials, fetch live sObject list
            if (!string.IsNullOrWhiteSpace(instanceUrl) && !string.IsNullOrWhiteSpace(accessToken))
            {
                try
                {
                    return await FetchSObjectListAsync(instanceUrl, accessToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Salesforce: failed to fetch live sObject list; returning defaults.");
                }
            }

            // Fall back to the well-known list
            return AllResources;
        }

        // ── SOQL query execution (with pagination) ───────────────────────────

        private async Task<List<object>> ExecuteQueryAsync(
            string instanceUrl, string accessToken, string soql, bool paginate = true)
        {
            var results = new List<object>();
            var baseUrl = BuildBaseUrl(instanceUrl);
            var url = $"{baseUrl}/query?q={Uri.EscapeDataString(soql)}";

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseRecords(doc.RootElement, results);

                // Pagination via nextRecordsUrl
                url = null!;
                if (paginate
                    && doc.RootElement.TryGetProperty("nextRecordsUrl", out var nextUrl)
                    && nextUrl.ValueKind == JsonValueKind.String)
                {
                    // nextRecordsUrl is a relative path like "/services/data/v59.0/query/01g..."
                    url = $"{instanceUrl.TrimEnd('/')}{nextUrl.GetString()}";
                }
            }
            while (url != null && paginate);

            _logger.LogInformation("Salesforce: read {Count} records via SOQL.", results.Count);
            return results;
        }

        // ── Schema discovery ─────────────────────────────────────────────────

        /// <summary>
        /// Calls the sObject Describe endpoint to retrieve all field names.
        /// GET /services/data/v59.0/sobjects/{resource}/describe/
        /// </summary>
        private async Task<List<string>> FetchFieldNamesAsync(
            string instanceUrl, string accessToken, string resource)
        {
            var baseUrl = BuildBaseUrl(instanceUrl);
            var url = $"{baseUrl}/sobjects/{Uri.EscapeDataString(resource)}/describe/";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var names = new List<string>();
            if (doc.RootElement.TryGetProperty("fields", out var fields)
                && fields.ValueKind == JsonValueKind.Array)
            {
                foreach (var field in fields.EnumerateArray())
                {
                    if (field.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                    {
                        names.Add(name.GetString()!);
                    }
                }
            }

            return names;
        }

        // ── sObject list ─────────────────────────────────────────────────────

        /// <summary>
        /// Fetches the list of all accessible sObjects from the Salesforce org.
        /// GET /services/data/v59.0/sobjects/
        /// </summary>
        private async Task<List<string>> FetchSObjectListAsync(
            string instanceUrl, string accessToken)
        {
            var baseUrl = BuildBaseUrl(instanceUrl);
            var url = $"{baseUrl}/sobjects/";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var names = new List<string>();
            if (doc.RootElement.TryGetProperty("sobjects", out var sobjects)
                && sobjects.ValueKind == JsonValueKind.Array)
            {
                foreach (var obj in sobjects.EnumerateArray())
                {
                    if (obj.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                    {
                        names.Add(name.GetString()!);
                    }
                }
            }

            return names;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseRecords(JsonElement root, List<object> results)
        {
            if (!root.TryGetProperty("records", out var records)
                || records.ValueKind != JsonValueKind.Array)
            {
                return;
            }

            foreach (var element in records.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    // Skip the "attributes" metadata object that Salesforce includes
                    if (prop.Name == "attributes")
                        continue;

                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                results.Add(row);
            }
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True   => true,
                JsonValueKind.False  => false,
                JsonValueKind.Null   => string.Empty,
                JsonValueKind.Object => FlattenNestedObject(value),
                JsonValueKind.Array  => value.ToString(),
                _                    => value.ToString()
            };
        }

        /// <summary>
        /// Flattens a nested Salesforce relationship object (e.g. Owner.Name) into a string.
        /// For simple nested objects with a "Name" field, returns the Name; otherwise serialises to JSON.
        /// </summary>
        private static object FlattenNestedObject(JsonElement element)
        {
            // Common pattern: relationship fields return { "attributes": {...}, "Name": "..." }
            if (element.TryGetProperty("Name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
                return nameEl.GetString() ?? string.Empty;

            return element.ToString();
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        // ── URL helpers ──────────────────────────────────────────────────────

        private static string BuildBaseUrl(string instanceUrl)
        {
            return $"{instanceUrl.TrimEnd('/')}/services/data/{ApiVersion}";
        }

        /// <summary>
        /// Strips a trailing LIMIT clause from a SOQL query (case-insensitive).
        /// </summary>
        private static string StripLimit(string soql)
        {
            // Match "LIMIT n" at the end of the query
            var idx = soql.LastIndexOf("LIMIT", StringComparison.OrdinalIgnoreCase);
            if (idx > 0)
            {
                return soql[..idx].TrimEnd();
            }
            return soql;
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"instanceUrl":"...","accessToken":"..."}
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

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (string.IsNullOrWhiteSpace(token))
                throw new ConnectorException(
                    "Salesforce access token is required. Provide it via Parameters['accessToken'] or the connection string JSON.",
                    new ArgumentException("Missing 'accessToken'."),
                    "salesforce");
            return token;
        }

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
                    $"Salesforce connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "salesforce");
            return value;
        }
    }
}
