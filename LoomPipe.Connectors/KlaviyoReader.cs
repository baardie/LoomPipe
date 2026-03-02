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
    /// Reads data from Klaviyo using the v2024-10-15 API.
    ///
    /// Parameters:
    ///   accessToken  — Klaviyo API key (used as "Klaviyo-API-Key {apiKey}" auth header)
    ///   resource     — profiles, lists, segments, campaigns, flows, metrics, events,
    ///                  templates, catalogs, tags
    ///
    /// Auth header: Authorization: Klaviyo-API-Key {apiKey}
    /// Required header: revision: 2024-10-15
    /// </summary>
    public class KlaviyoReader : ISourceReader
    {
        private const string BaseUrl = "https://a.klaviyo.com/api";
        private const string ApiRevision = "2024-10-15";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "profiles", "lists", "segments", "campaigns", "flows",
            "metrics", "events", "templates", "catalogs", "tags"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<KlaviyoReader> _logger;

        public KlaviyoReader(HttpClient httpClient, ILogger<KlaviyoReader> logger)
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
                    "Klaviyo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "klaviyo");

            _logger.LogInformation("Klaviyo: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadPaginatedAsync(accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Klaviyo: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Klaviyo resource '{resource}': {ex.Message}", ex, "klaviyo");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Klaviyo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "klaviyo");

            _logger.LogInformation("Klaviyo: discovering schema for '{Resource}'.", resource);

            try
            {
                // Read one page and derive field names from the first record.
                var sample = await ReadPaginatedAsync(accessToken, resource, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Klaviyo: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Klaviyo schema for '{resource}': {ex.Message}", ex, "klaviyo");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Klaviyo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "klaviyo");

            _logger.LogInformation("Klaviyo: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(accessToken, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Klaviyo: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Klaviyo dry run preview failed for '{resource}': {ex.Message}", ex, "klaviyo");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string accessToken, string resource, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextUrl = $"{BaseUrl}/{resource}/?page[size]={PageSize}";
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Klaviyo JSON:API response: { "data": [...], "links": { "next": "url" } }
                if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in data.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        // Top-level fields: id, type
                        if (element.TryGetProperty("id", out var id))
                            row["id"] = id.GetString() ?? id.ToString();

                        if (element.TryGetProperty("type", out var type))
                            row["type"] = type.GetString() ?? type.ToString();

                        // Flatten "attributes" sub-object into the row.
                        if (element.TryGetProperty("attributes", out var attrs) && attrs.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in attrs.EnumerateObject())
                            {
                                row[prop.Name] = ConvertJsonValue(prop.Value);
                            }
                        }

                        // Flatten "relationships" keys as references.
                        if (element.TryGetProperty("relationships", out var rels) && rels.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var rel in rels.EnumerateObject())
                            {
                                if (rel.Value.TryGetProperty("data", out var relData))
                                {
                                    if (relData.ValueKind == JsonValueKind.Object && relData.TryGetProperty("id", out var relId))
                                    {
                                        row[$"{rel.Name}_id"] = relId.GetString() ?? relId.ToString();
                                    }
                                    else if (relData.ValueKind == JsonValueKind.Array)
                                    {
                                        var ids = new List<string>();
                                        foreach (var item in relData.EnumerateArray())
                                        {
                                            if (item.TryGetProperty("id", out var itemId))
                                                ids.Add(itemId.GetString() ?? itemId.ToString());
                                        }
                                        row[$"{rel.Name}_ids"] = string.Join(",", ids);
                                    }
                                }
                            }
                        }

                        results.Add(row);
                    }
                }

                // Cursor-based pagination via links.next
                nextUrl = null;
                if (doc.RootElement.TryGetProperty("links", out var links)
                    && links.TryGetProperty("next", out var nextLink)
                    && nextLink.ValueKind == JsonValueKind.String)
                {
                    nextUrl = nextLink.GetString();
                    if (string.IsNullOrEmpty(nextUrl))
                        nextUrl = null;
                }

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Klaviyo: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

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
            request.Headers.Authorization = new AuthenticationHeaderValue("Klaviyo-API-Key", accessToken);
            request.Headers.TryAddWithoutValidation("revision", ApiRevision);
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
                    $"Klaviyo connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "klaviyo");
            return value;
        }
    }
}
