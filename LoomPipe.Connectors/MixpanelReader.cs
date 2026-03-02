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
    /// Reads analytics data from Mixpanel using the Export and Query APIs.
    ///
    /// Parameters:
    ///   accessToken  — service account secret (used as password for Basic auth)
    ///   username     — service account username (used as username for Basic auth)
    ///   projectId    — Mixpanel project ID
    ///   resource     — events, funnels, retention, people, cohorts, engage
    ///   startDate    — ISO date (yyyy-MM-dd) for export range start
    ///   endDate      — ISO date (yyyy-MM-dd) for export range end
    /// </summary>
    public class MixpanelReader : ISourceReader
    {
        private const string ExportBaseUrl = "https://data.mixpanel.com/api/2.0";
        private const string QueryBaseUrl = "https://mixpanel.com/api/2.0";
        private const int EngagePageSize = 1000;

        private static readonly string[] AllResources =
        {
            "events", "funnels", "retention", "people", "cohorts", "engage"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MixpanelReader> _logger;

        public MixpanelReader(HttpClient httpClient, ILogger<MixpanelReader> logger)
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
                    "Mixpanel access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "mixpanel");

            _logger.LogInformation("Mixpanel: reading resource '{Resource}'.", resource);

            try
            {
                return resource switch
                {
                    "events"    => await ReadEventsAsync(config, accessToken),
                    "engage" or "people" => await ReadEngageAsync(config, accessToken),
                    _           => await ReadQueryAsync(config, accessToken, resource)
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mixpanel: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Mixpanel resource '{resource}': {ex.Message}", ex, "mixpanel");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Mixpanel access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "mixpanel");

            _logger.LogInformation("Mixpanel: discovering schema for '{Resource}'.", resource);

            try
            {
                // Read a small sample and derive field names from the first record.
                var sample = (await ReadAsync(config)).ToList();
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mixpanel: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Mixpanel schema for '{resource}': {ex.Message}", ex, "mixpanel");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Mixpanel: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mixpanel: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Mixpanel dry run preview failed for '{resource}': {ex.Message}", ex, "mixpanel");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Events export (JSONL) ────────────────────────────────────────────

        private async Task<List<object>> ReadEventsAsync(DataSourceConfig config, string accessToken)
        {
            var startDate = GetStringParam(config.Parameters, "startDate")
                ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
            var endDate = GetStringParam(config.Parameters, "endDate")
                ?? DateTime.UtcNow.ToString("yyyy-MM-dd");

            var url = $"{ExportBaseUrl}/export?from_date={Uri.EscapeDataString(startDate)}&to_date={Uri.EscapeDataString(endDate)}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, config, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            // Export API returns newline-delimited JSON (JSONL).
            var body = await response.Content.ReadAsStringAsync();
            var results = new List<object>();

            foreach (var line in body.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    IDictionary<string, object> row = new ExpandoObject();

                    if (doc.RootElement.TryGetProperty("event", out var eventName))
                        row["event"] = eventName.GetString() ?? string.Empty;

                    if (doc.RootElement.TryGetProperty("properties", out var props) && props.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var prop in props.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                    }

                    results.Add(row);
                }
                catch (JsonException)
                {
                    // Skip malformed lines.
                }
            }

            _logger.LogInformation("Mixpanel: read {Count} events.", results.Count);
            return results;
        }

        // ── Engage / People (paginated) ──────────────────────────────────────

        private async Task<List<object>> ReadEngageAsync(DataSourceConfig config, string accessToken)
        {
            var results = new List<object>();
            string? sessionId = null;
            int page = 0;

            do
            {
                var url = $"{QueryBaseUrl}/engage?page_size={EngagePageSize}";
                if (sessionId != null)
                    url += $"&session_id={Uri.EscapeDataString(sessionId)}&page={page}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyBasicAuth(request, config, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                if (doc.RootElement.TryGetProperty("results", out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        if (element.TryGetProperty("$distinct_id", out var distinctId))
                            row["distinct_id"] = distinctId.GetString() ?? string.Empty;

                        if (element.TryGetProperty("$properties", out var props) && props.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in props.EnumerateObject())
                            {
                                row[prop.Name] = ConvertJsonValue(prop.Value);
                            }
                        }

                        results.Add(row);
                    }

                    if (items.GetArrayLength() < EngagePageSize)
                        break;
                }
                else
                {
                    break;
                }

                // Pagination via session_id
                if (doc.RootElement.TryGetProperty("session_id", out var sid) && sid.ValueKind == JsonValueKind.String)
                    sessionId = sid.GetString();
                else
                    break;

                page++;
            }
            while (true);

            _logger.LogInformation("Mixpanel: read {Count} engage profiles.", results.Count);
            return results;
        }

        // ── Generic query endpoint ───────────────────────────────────────────

        private async Task<List<object>> ReadQueryAsync(DataSourceConfig config, string accessToken, string resource)
        {
            var url = $"{QueryBaseUrl}/{Uri.EscapeDataString(resource)}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, config, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();
            FlattenJsonElement(doc.RootElement, results);

            _logger.LogInformation("Mixpanel: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void FlattenJsonElement(JsonElement root, List<object> results)
        {
            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in root.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    if (element.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                    }
                    results.Add(row);
                }
            }
            else if (root.ValueKind == JsonValueKind.Object)
            {
                // Some endpoints return a single object with nested data.
                if (root.TryGetProperty("results", out var nested) && nested.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonElement(nested, results);
                }
                else if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonElement(data, results);
                }
                else
                {
                    // Treat the object itself as a single record.
                    IDictionary<string, object> row = new ExpandoObject();
                    foreach (var prop in root.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                    results.Add(row);
                }
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

        private static void ApplyBasicAuth(HttpRequestMessage request, DataSourceConfig config, string accessToken)
        {
            var username = GetStringParam(config.Parameters, "username") ?? string.Empty;
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{accessToken}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
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
                    $"Mixpanel connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "mixpanel");
            return value;
        }
    }
}
