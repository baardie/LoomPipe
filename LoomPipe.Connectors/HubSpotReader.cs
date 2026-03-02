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
    /// Reads CRM objects from HubSpot using the v3 API.
    ///
    /// Parameters:
    ///   accessToken  — HubSpot private app access token or OAuth token
    ///   resource     — contacts, companies, deals, tickets, leads, products, line_items,
    ///                  quotes, calls, emails, meetings, notes, tasks, owners, pipelines
    ///   properties   — optional comma-separated list of properties to fetch
    ///   startDate    — optional ISO date for filtering (incremental via lastmodifieddate)
    /// </summary>
    public class HubSpotReader : ISourceReader
    {
        private const string BaseUrl = "https://api.hubapi.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "contacts", "companies", "deals", "tickets", "leads",
            "products", "line_items", "quotes",
            "calls", "emails", "meetings", "notes", "tasks",
            "owners", "pipelines"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<HubSpotReader> _logger;

        public HubSpotReader(HttpClient httpClient, ILogger<HubSpotReader> logger)
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
                    "HubSpot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "hubspot");
            var properties  = GetStringParam(config.Parameters, "properties");
            var startDate   = GetStringParam(config.Parameters, "startDate");

            _logger.LogInformation("HubSpot: reading resource '{Resource}'.", resource);

            try
            {
                // If a watermark is provided, use the search API for incremental loads.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    return await ReadIncrementalAsync(resource, accessToken, properties, watermarkField, watermarkValue);
                }

                // If startDate is configured, treat it as an incremental load via lastmodifieddate.
                if (!string.IsNullOrEmpty(startDate))
                {
                    return await ReadIncrementalAsync(resource, accessToken, properties, "lastmodifieddate", startDate);
                }

                return await ReadFullAsync(resource, accessToken, properties);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "HubSpot: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read HubSpot resource '{resource}': {ex.Message}", ex, "hubspot");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "HubSpot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "hubspot");

            _logger.LogInformation("HubSpot: discovering schema for '{Resource}'.", resource);

            try
            {
                // owners and pipelines don't have a properties endpoint — fall back to reading a sample.
                if (resource is "owners" or "pipelines")
                {
                    var sample = await ReadFullAsync(resource, accessToken, properties: null, maxPages: 1);
                    var first = sample.FirstOrDefault() as IDictionary<string, object>;
                    return first?.Keys ?? Array.Empty<string>();
                }

                var objectType = NormalizeObjectType(resource);
                var url = $"{BaseUrl}/crm/v3/properties/{objectType}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var names = new List<string>();
                if (doc.RootElement.TryGetProperty("results", out var results)
                    && results.ValueKind == JsonValueKind.Array)
                {
                    foreach (var prop in results.EnumerateArray())
                    {
                        if (prop.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                        {
                            names.Add(name.GetString()!);
                        }
                    }
                }

                return names;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "HubSpot: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover HubSpot schema for '{resource}': {ex.Message}", ex, "hubspot");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "HubSpot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "hubspot");
            var properties  = GetStringParam(config.Parameters, "properties");

            _logger.LogInformation("HubSpot: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                // Fetch only one page and take the sample.
                var records = await ReadFullAsync(resource, accessToken, properties, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "HubSpot: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"HubSpot dry run preview failed for '{resource}': {ex.Message}", ex, "hubspot");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? properties, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? after = null;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, properties, after);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, resource, results);

                // Pagination
                after = null;
                if (doc.RootElement.TryGetProperty("paging", out var paging)
                    && paging.TryGetProperty("next", out var next)
                    && next.TryGetProperty("after", out var afterEl)
                    && afterEl.ValueKind == JsonValueKind.String)
                {
                    after = afterEl.GetString();
                }

                page++;
            }
            while (after != null && page < maxPages);

            _logger.LogInformation("HubSpot: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Incremental read (search API) ────────────────────────────────────

        private async Task<List<object>> ReadIncrementalAsync(
            string resource, string accessToken, string? properties,
            string watermarkField, string watermarkValue)
        {
            // owners and pipelines don't support the search API — fall back to full read and filter client-side.
            if (resource is "owners" or "pipelines")
            {
                _logger.LogWarning("HubSpot: incremental read not supported for '{Resource}'; falling back to full read.", resource);
                var all = await ReadFullAsync(resource, accessToken, properties);
                return all; // return everything; pipeline engine handles watermark compare
            }

            var objectType = NormalizeObjectType(resource);
            var url = $"{BaseUrl}/crm/v3/objects/{objectType}/search";
            var results = new List<object>();
            string? after = null;

            do
            {
                var body = BuildSearchBody(watermarkField, watermarkValue, properties, after);

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, resource, results);

                // Pagination
                after = null;
                if (doc.RootElement.TryGetProperty("paging", out var paging)
                    && paging.TryGetProperty("next", out var next)
                    && next.TryGetProperty("after", out var afterEl)
                    && afterEl.ValueKind == JsonValueKind.String)
                {
                    after = afterEl.GetString();
                }
            }
            while (after != null);

            _logger.LogInformation("HubSpot: incremental read returned {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── URL builders ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? properties, string? after)
        {
            string baseEndpoint = resource switch
            {
                "owners"    => $"{BaseUrl}/crm/v3/owners",
                "pipelines" => $"{BaseUrl}/crm/v3/pipelines/deals",
                _           => $"{BaseUrl}/crm/v3/objects/{NormalizeObjectType(resource)}"
            };

            var sb = new StringBuilder(baseEndpoint);
            sb.Append($"?limit={PageLimit}");

            if (!string.IsNullOrEmpty(properties) && resource is not "owners" and not "pipelines")
            {
                sb.Append($"&properties={Uri.EscapeDataString(properties)}");
            }

            if (!string.IsNullOrEmpty(after))
            {
                sb.Append($"&after={Uri.EscapeDataString(after)}");
            }

            return sb.ToString();
        }

        private static string BuildSearchBody(
            string filterField, string filterValue, string? properties, string? after)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();

            // filterGroups
            writer.WritePropertyName("filterGroups");
            writer.WriteStartArray();
            writer.WriteStartObject();
            writer.WritePropertyName("filters");
            writer.WriteStartArray();
            writer.WriteStartObject();
            writer.WriteString("propertyName", filterField);
            writer.WriteString("operator", "GTE");
            writer.WriteString("value", filterValue);
            writer.WriteEndObject();
            writer.WriteEndArray();
            writer.WriteEndObject();
            writer.WriteEndArray();

            // properties
            if (!string.IsNullOrEmpty(properties))
            {
                writer.WritePropertyName("properties");
                writer.WriteStartArray();
                foreach (var prop in properties.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    writer.WriteStringValue(prop);
                }
                writer.WriteEndArray();
            }

            writer.WriteNumber("limit", PageLimit);

            if (!string.IsNullOrEmpty(after))
            {
                writer.WriteString("after", after);
            }

            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard response shape
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

                // Top-level scalar fields: id, createdAt, updatedAt
                if (element.TryGetProperty("id", out var id))
                    row["id"] = id.GetString() ?? id.ToString();

                if (element.TryGetProperty("createdAt", out var createdAt))
                    row["createdAt"] = createdAt.GetString() ?? createdAt.ToString();

                if (element.TryGetProperty("updatedAt", out var updatedAt))
                    row["updatedAt"] = updatedAt.GetString() ?? updatedAt.ToString();

                // Flatten the "properties" sub-object into the row.
                if (element.TryGetProperty("properties", out var props) && props.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in props.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                // For resources without a "properties" sub-object (owners, pipelines),
                // flatten all top-level fields that we haven't already captured.
                if (resource is "owners" or "pipelines")
                {
                    foreach (var prop in element.EnumerateObject())
                    {
                        if (!row.ContainsKey(prop.Name))
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                    }
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

        // ── Object-type normalisation ────────────────────────────────────────

        /// <summary>
        /// Normalises the resource name into the HubSpot CRM API object type path segment.
        /// Most resources map 1:1; only special cases are owners and pipelines.
        /// </summary>
        private static string NormalizeObjectType(string resource) => resource switch
        {
            "owners"    => "owners",
            "pipelines" => "pipelines",
            _           => resource
        };

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
                    $"HubSpot connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "hubspot");
            return value;
        }
    }
}
