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
    /// Reads data from Segment using the Profile API and Public API.
    ///
    /// Parameters:
    ///   accessToken  — Segment API key (Bearer token)
    ///   resource     — users, events, traits, sources, destinations, warehouses
    ///   spaceId      — Segment space ID (required for Profile API resources)
    /// </summary>
    public class SegmentReader : ISourceReader
    {
        private const string ProfileBaseUrl = "https://profiles.segment.com/v1";
        private const string PublicBaseUrl = "https://api.segmentapis.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "users", "events", "traits", "sources", "destinations", "warehouses"
        };

        // Resources served by the Public API (no spaceId needed).
        private static readonly HashSet<string> PublicApiResources = new(StringComparer.OrdinalIgnoreCase)
        {
            "sources", "destinations", "warehouses"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SegmentReader> _logger;

        public SegmentReader(HttpClient httpClient, ILogger<SegmentReader> logger)
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
                    "Segment access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "segment");

            _logger.LogInformation("Segment: reading resource '{Resource}'.", resource);

            try
            {
                if (PublicApiResources.Contains(resource))
                {
                    return await ReadPublicApiAsync(accessToken, resource);
                }

                return await ReadProfileApiAsync(config, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Segment: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Segment resource '{resource}': {ex.Message}", ex, "segment");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Segment: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = (await ReadAsync(config)).ToList();
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Segment: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Segment schema for '{resource}': {ex.Message}", ex, "segment");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Segment: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Segment: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Segment dry run preview failed for '{resource}': {ex.Message}", ex, "segment");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Public API (sources, destinations, warehouses) ───────────────────

        private async Task<List<object>> ReadPublicApiAsync(string accessToken, string resource)
        {
            var results = new List<object>();
            string? cursor = null;

            do
            {
                var url = $"{PublicBaseUrl}/{resource}?pagination.count={PageLimit}";
                if (!string.IsNullOrEmpty(cursor))
                    url += $"&pagination.cursor={Uri.EscapeDataString(cursor)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Response: { "data": { "sources": [...], ... }, "pagination": { "next": "..." } }
                if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Object)
                {
                    // The array is keyed by the resource name.
                    if (data.TryGetProperty(resource, out var items) && items.ValueKind == JsonValueKind.Array)
                    {
                        ParseArray(items, results);
                    }
                    else
                    {
                        // Try to find any array in the data object.
                        foreach (var prop in data.EnumerateObject())
                        {
                            if (prop.Value.ValueKind == JsonValueKind.Array)
                            {
                                ParseArray(prop.Value, results);
                                break;
                            }
                        }
                    }
                }

                // Cursor-based pagination
                cursor = null;
                if (doc.RootElement.TryGetProperty("pagination", out var pagination)
                    && pagination.TryGetProperty("next", out var next)
                    && next.ValueKind == JsonValueKind.String)
                {
                    cursor = next.GetString();
                    if (string.IsNullOrEmpty(cursor))
                        cursor = null;
                }
            }
            while (cursor != null);

            _logger.LogInformation("Segment: read {Count} records from Public API '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── Profile API (users, events, traits) ─────────────────────────────

        private async Task<List<object>> ReadProfileApiAsync(DataSourceConfig config, string accessToken, string resource)
        {
            var spaceId = GetStringParam(config.Parameters, "spaceId")
                ?? throw new ConnectorException(
                    "Segment 'spaceId' parameter is required for Profile API resources.",
                    new ArgumentException("Missing 'spaceId'."),
                    "segment");

            var collection = resource switch
            {
                "users"  => "users",
                "events" => "events",
                "traits" => "users",
                _        => "users"
            };

            var results = new List<object>();
            string? cursor = null;

            do
            {
                var url = $"{ProfileBaseUrl}/spaces/{Uri.EscapeDataString(spaceId)}/collections/{collection}/profiles?limit={PageLimit}";
                if (!string.IsNullOrEmpty(cursor))
                    url += $"&cursor={Uri.EscapeDataString(cursor)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Response: { "data": [...], "cursor": { "url": "...", "has_more": true } }
                if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(data, results);
                }

                // Cursor-based pagination
                cursor = null;
                if (doc.RootElement.TryGetProperty("cursor", out var cursorObj) && cursorObj.ValueKind == JsonValueKind.Object)
                {
                    if (cursorObj.TryGetProperty("has_more", out var hasMore) && hasMore.ValueKind == JsonValueKind.True
                        && cursorObj.TryGetProperty("url", out var urlEl) && urlEl.ValueKind == JsonValueKind.String)
                    {
                        cursor = urlEl.GetString();
                    }
                }
            }
            while (cursor != null);

            _logger.LogInformation("Segment: read {Count} records from Profile API '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseArray(JsonElement array, List<object> results)
        {
            foreach (var element in array.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                if (element.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in element.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Object && prop.Name == "traits")
                        {
                            // Flatten traits into the row.
                            foreach (var trait in prop.Value.EnumerateObject())
                            {
                                row[trait.Name] = ConvertJsonValue(trait.Value);
                            }
                        }
                        else
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
                    $"Segment connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "segment");
            return value;
        }
    }
}
