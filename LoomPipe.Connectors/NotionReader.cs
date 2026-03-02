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
    /// Reads data from the Notion API v1.
    ///
    /// Parameters:
    ///   accessToken  — Notion integration token (internal integration secret)
    ///   resource     — databases, pages, blocks, users, search
    ///   databaseId   — required when resource is "databases" (queries a specific database)
    /// </summary>
    public class NotionReader : ISourceReader
    {
        private const string BaseUrl = "https://api.notion.com/v1";
        private const string NotionVersion = "2022-06-28";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "databases", "pages", "blocks", "users", "search"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<NotionReader> _logger;

        public NotionReader(HttpClient httpClient, ILogger<NotionReader> logger)
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
                    "Notion access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "notion");

            _logger.LogInformation("Notion: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, config.Parameters);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Notion: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Notion resource '{resource}': {ex.Message}", ex, "notion");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Notion access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "notion");

            _logger.LogInformation("Notion: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, config.Parameters, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Notion: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Notion schema for '{resource}': {ex.Message}", ex, "notion");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Notion access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "notion");

            _logger.LogInformation("Notion: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, config.Parameters, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Notion: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Notion dry run preview failed for '{resource}': {ex.Message}", ex, "notion");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, Dictionary<string, object> parameters, int maxPages = int.MaxValue)
        {
            var results = new List<object>();

            switch (resource)
            {
                case "databases":
                    var databaseId = GetRequiredParam(parameters, "databaseId");
                    await ReadDatabaseQueryAsync(databaseId, accessToken, results, maxPages);
                    break;

                case "pages":
                    await ReadSearchAsync("page", accessToken, results, maxPages);
                    break;

                case "blocks":
                    await ReadSearchAsync("block", accessToken, results, maxPages);
                    break;

                case "users":
                    await ReadUsersAsync(accessToken, results, maxPages);
                    break;

                case "search":
                    await ReadSearchAsync(null, accessToken, results, maxPages);
                    break;

                default:
                    throw new ConnectorException(
                        $"Notion: unsupported resource '{resource}'.",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "notion");
            }

            _logger.LogInformation("Notion: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── Database query (POST with pagination) ────────────────────────────

        private async Task ReadDatabaseQueryAsync(
            string databaseId, string accessToken, List<object> results, int maxPages)
        {
            string? startCursor = null;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/databases/{Uri.EscapeDataString(databaseId)}/query";

                using var ms = new System.IO.MemoryStream();
                using var writer = new Utf8JsonWriter(ms);
                writer.WriteStartObject();
                writer.WriteNumber("page_size", PageSize);
                if (!string.IsNullOrEmpty(startCursor))
                    writer.WriteString("start_cursor", startCursor);
                writer.WriteEndObject();
                writer.Flush();

                var body = Encoding.UTF8.GetString(ms.ToArray());

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsArray(doc.RootElement, results);

                startCursor = null;
                if (doc.RootElement.TryGetProperty("has_more", out var hasMore)
                    && hasMore.GetBoolean()
                    && doc.RootElement.TryGetProperty("next_cursor", out var cursor)
                    && cursor.ValueKind == JsonValueKind.String)
                {
                    startCursor = cursor.GetString();
                }

                page++;
            }
            while (startCursor != null && page < maxPages);
        }

        // ── Search API (POST with pagination) ────────────────────────────────

        private async Task ReadSearchAsync(
            string? filterType, string accessToken, List<object> results, int maxPages)
        {
            string? startCursor = null;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/search";

                using var ms = new System.IO.MemoryStream();
                using var writer = new Utf8JsonWriter(ms);
                writer.WriteStartObject();
                writer.WriteNumber("page_size", PageSize);

                if (!string.IsNullOrEmpty(filterType))
                {
                    writer.WritePropertyName("filter");
                    writer.WriteStartObject();
                    writer.WriteString("value", filterType);
                    writer.WriteString("property", "object");
                    writer.WriteEndObject();
                }

                if (!string.IsNullOrEmpty(startCursor))
                    writer.WriteString("start_cursor", startCursor);

                writer.WriteEndObject();
                writer.Flush();

                var body = Encoding.UTF8.GetString(ms.ToArray());

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsArray(doc.RootElement, results);

                startCursor = null;
                if (doc.RootElement.TryGetProperty("has_more", out var hasMore)
                    && hasMore.GetBoolean()
                    && doc.RootElement.TryGetProperty("next_cursor", out var cursor)
                    && cursor.ValueKind == JsonValueKind.String)
                {
                    startCursor = cursor.GetString();
                }

                page++;
            }
            while (startCursor != null && page < maxPages);
        }

        // ── Users list (GET with pagination) ─────────────────────────────────

        private async Task ReadUsersAsync(string accessToken, List<object> results, int maxPages)
        {
            string? startCursor = null;
            int page = 0;

            do
            {
                var sb = new StringBuilder($"{BaseUrl}/users?page_size={PageSize}");
                if (!string.IsNullOrEmpty(startCursor))
                    sb.Append($"&start_cursor={Uri.EscapeDataString(startCursor)}");

                using var request = new HttpRequestMessage(HttpMethod.Get, sb.ToString());
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsArray(doc.RootElement, results);

                startCursor = null;
                if (doc.RootElement.TryGetProperty("has_more", out var hasMore)
                    && hasMore.GetBoolean()
                    && doc.RootElement.TryGetProperty("next_cursor", out var cursor)
                    && cursor.ValueKind == JsonValueKind.String)
                {
                    startCursor = cursor.GetString();
                }

                page++;
            }
            while (startCursor != null && page < maxPages);
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsArray(JsonElement root, List<object> results)
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

                // Flatten all top-level scalar fields.
                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object || prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        // For nested "properties" in database results, flatten them.
                        if (prop.Name == "properties" && prop.Value.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var dbProp in prop.Value.EnumerateObject())
                            {
                                row[$"prop_{dbProp.Name}"] = ExtractNotionPropertyValue(dbProp.Value);
                            }
                        }
                        else
                        {
                            row[prop.Name] = prop.Value.ToString();
                        }
                    }
                    else
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
            }
        }

        /// <summary>
        /// Extracts a displayable value from a Notion database property object.
        /// Notion property values are wrapped in a type envelope — this unwraps the most common types.
        /// </summary>
        private static object ExtractNotionPropertyValue(JsonElement propValue)
        {
            if (propValue.TryGetProperty("type", out var typeEl) && typeEl.ValueKind == JsonValueKind.String)
            {
                var type = typeEl.GetString();
                if (type != null && propValue.TryGetProperty(type, out var inner))
                {
                    return inner.ValueKind switch
                    {
                        JsonValueKind.String => inner.GetString() ?? string.Empty,
                        JsonValueKind.Number => inner.TryGetInt64(out var l) ? (object)l : inner.GetDouble(),
                        JsonValueKind.True   => true,
                        JsonValueKind.False  => false,
                        JsonValueKind.Null   => string.Empty,
                        JsonValueKind.Array  => ExtractArrayValues(inner),
                        _                    => inner.ToString()
                    };
                }
            }

            return propValue.ToString();
        }

        private static string ExtractArrayValues(JsonElement array)
        {
            var values = new List<string>();
            foreach (var item in array.EnumerateArray())
            {
                if (item.TryGetProperty("plain_text", out var plainText))
                    values.Add(plainText.GetString() ?? string.Empty);
                else if (item.TryGetProperty("name", out var name))
                    values.Add(name.GetString() ?? string.Empty);
                else
                    values.Add(item.ToString());
            }
            return string.Join(", ", values);
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
            request.Headers.Add("Notion-Version", NotionVersion);
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
                    $"Notion connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "notion");
            return value;
        }
    }
}
