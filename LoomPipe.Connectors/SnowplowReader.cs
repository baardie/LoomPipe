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
    /// Reads enriched analytics events from Snowplow Micro or a compatible Snowplow API endpoint.
    ///
    /// Parameters:
    ///   accessToken  — optional Bearer token for authenticated Snowplow endpoints
    ///   host         — Snowplow collector/Micro URL (e.g. "http://localhost:9090")
    ///   resource     — events, good_events, bad_events, schemas
    ///
    /// Snowplow Micro endpoints:
    ///   GET /micro/all   — all enriched events
    ///   GET /micro/good  — successfully validated events
    ///   GET /micro/bad   — events that failed validation
    ///
    /// For production use, the connectionString can point to a warehouse query endpoint.
    /// </summary>
    public class SnowplowReader : ISourceReader
    {
        private static readonly string[] AllResources =
        {
            "events", "good_events", "bad_events", "schemas"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SnowplowReader> _logger;

        public SnowplowReader(HttpClient httpClient, ILogger<SnowplowReader> logger)
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
            var resource = GetRequiredParam(config.Parameters, "resource");
            var host     = ResolveHost(config);

            _logger.LogInformation("Snowplow: reading resource '{Resource}' from host '{Host}'.", resource, host);

            try
            {
                var records = await ReadEventsAsync(host, config, resource);

                // Client-side watermark filtering if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = FilterByWatermark(records, watermarkField, watermarkValue);
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Snowplow: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Snowplow resource '{resource}': {ex.Message}", ex, "snowplow");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var host     = ResolveHost(config);

            _logger.LogInformation("Snowplow: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadEventsAsync(host, config, resource);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Snowplow: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Snowplow schema for '{resource}': {ex.Message}", ex, "snowplow");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var host     = ResolveHost(config);

            _logger.LogInformation("Snowplow: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadEventsAsync(host, config, resource);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Snowplow: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Snowplow dry run preview failed for '{resource}': {ex.Message}", ex, "snowplow");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Event read ───────────────────────────────────────────────────────

        private async Task<List<object>> ReadEventsAsync(
            string host, DataSourceConfig config, string resource)
        {
            var url = BuildUrl(host, resource);

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, config);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();
            ParseResults(doc.RootElement, results);

            _logger.LogInformation("Snowplow: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string host, string resource)
        {
            // Normalise host: remove trailing slash.
            host = host.TrimEnd('/');

            return resource switch
            {
                "events"      => $"{host}/micro/all",
                "good_events" => $"{host}/micro/good",
                "bad_events"  => $"{host}/micro/bad",
                "schemas"     => $"{host}/micro/all",  // schemas extracted from event data
                _             => $"{host}/micro/all"
            };
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResults(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else if (root.TryGetProperty("events", out var events) && events.ValueKind == JsonValueKind.Array)
            {
                items = events;
            }
            else if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
            {
                items = data;
            }
            else if (root.TryGetProperty("result", out var result) && result.ValueKind == JsonValueKind.Array)
            {
                items = result;
            }
            else
            {
                // Single object response — wrap it.
                if (root.ValueKind == JsonValueKind.Object)
                {
                    results.Add(FlattenElement(root));
                }
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                results.Add(FlattenElement(element));
            }
        }

        private static object FlattenElement(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in element.EnumerateObject())
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

        // ── Watermark filtering ──────────────────────────────────────────────

        private static List<object> FilterByWatermark(
            List<object> records, string watermarkField, string watermarkValue)
        {
            return records.Where(r =>
            {
                if (r is IDictionary<string, object> dict && dict.TryGetValue(watermarkField, out var val))
                {
                    var valStr = val?.ToString() ?? string.Empty;
                    return string.Compare(valStr, watermarkValue, StringComparison.Ordinal) > 0;
                }
                return true; // include records without the watermark field
            }).ToList();
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private void ApplyAuth(HttpRequestMessage request, DataSourceConfig config)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken");

            if (!string.IsNullOrEmpty(accessToken))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            }
            // Snowplow Micro typically doesn't require auth — no error if missing.
        }

        private string ResolveHost(DataSourceConfig config)
        {
            var host = GetStringParam(config.Parameters, "host");
            if (!string.IsNullOrEmpty(host)) return host;

            // Fall back to connectionString.
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                // Try JSON format.
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    if (doc.RootElement.TryGetProperty("host", out var h))
                        return h.GetString()
                            ?? throw new ConnectorException(
                                "Snowplow 'host' is required.",
                                new ArgumentException("Missing 'host'."),
                                "snowplow");
                }
                catch (JsonException)
                {
                    // ConnectionString is not JSON — treat as host URL.
                    return config.ConnectionString;
                }
            }

            throw new ConnectorException(
                "Snowplow 'host' parameter is required (e.g. 'http://localhost:9090' for Snowplow Micro).",
                new ArgumentException("Missing 'host'."),
                "snowplow");
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
                    $"Snowplow connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "snowplow");
            return value;
        }
    }
}
