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
    /// Reads analytics data from Amplitude using the Dashboard REST API.
    ///
    /// Parameters:
    ///   apiKey       — Amplitude API Key (also used as Basic auth username)
    ///   secretKey    — Amplitude Secret Key (also used as Basic auth password)
    ///   resource     — events, cohorts, user_activity, revenue, annotations
    ///   startDate    — ISO date/time (yyyyMMddTHH) for export range start
    ///   endDate      — ISO date/time (yyyyMMddTHH) for export range end
    ///
    /// ConnectionString JSON: {"apiKey":"...","secretKey":"..."}
    /// </summary>
    public class AmplitudeReader : ISourceReader
    {
        private const string BaseUrl = "https://amplitude.com/api/2";

        private static readonly string[] AllResources =
        {
            "events", "cohorts", "user_activity", "revenue", "annotations"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<AmplitudeReader> _logger;

        public AmplitudeReader(HttpClient httpClient, ILogger<AmplitudeReader> logger)
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
            var (apiKey, secretKey) = ResolveCredentials(config);

            _logger.LogInformation("Amplitude: reading resource '{Resource}'.", resource);

            try
            {
                return resource switch
                {
                    "events"        => await ReadEventsAsync(config, apiKey, secretKey),
                    "cohorts"       => await ReadGenericAsync(apiKey, secretKey, "/cohorts"),
                    "user_activity" => await ReadUserActivityAsync(config, apiKey, secretKey),
                    "revenue"       => await ReadRevenueAsync(config, apiKey, secretKey),
                    "annotations"   => await ReadGenericAsync(apiKey, secretKey, "/annotations"),
                    _               => await ReadGenericAsync(apiKey, secretKey, $"/{resource}")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Amplitude: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Amplitude resource '{resource}': {ex.Message}", ex, "amplitude");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Amplitude: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = (await ReadAsync(config)).ToList();
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Amplitude: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Amplitude schema for '{resource}': {ex.Message}", ex, "amplitude");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Amplitude: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Amplitude: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Amplitude dry run preview failed for '{resource}': {ex.Message}", ex, "amplitude");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Events (segmentation API) ────────────────────────────────────────

        private async Task<List<object>> ReadEventsAsync(DataSourceConfig config, string apiKey, string secretKey)
        {
            var startDate = GetStringParam(config.Parameters, "startDate")
                ?? DateTime.UtcNow.AddDays(-30).ToString("yyyyMMdd") + "T00";
            var endDate = GetStringParam(config.Parameters, "endDate")
                ?? DateTime.UtcNow.ToString("yyyyMMdd") + "T23";

            var url = $"{BaseUrl}/events/segmentation?e=%7B%22event_type%22%3A%22_all%22%7D&start={Uri.EscapeDataString(startDate)}&end={Uri.EscapeDataString(endDate)}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, apiKey, secretKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("data", out var data) && data.TryGetProperty("series", out var series))
            {
                // Flatten series data into rows.
                if (series.ValueKind == JsonValueKind.Object)
                {
                    foreach (var eventType in series.EnumerateObject())
                    {
                        if (eventType.Value.ValueKind == JsonValueKind.Array)
                        {
                            int index = 0;
                            foreach (var val in eventType.Value.EnumerateArray())
                            {
                                IDictionary<string, object> row = new ExpandoObject();
                                row["event_type"] = eventType.Name;
                                row["index"] = index;
                                row["value"] = ConvertJsonValue(val);
                                results.Add(row);
                                index++;
                            }
                        }
                    }
                }
            }
            else
            {
                FlattenJsonElement(doc.RootElement, results);
            }

            _logger.LogInformation("Amplitude: read {Count} event records.", results.Count);
            return results;
        }

        // ── User activity ────────────────────────────────────────────────────

        private async Task<List<object>> ReadUserActivityAsync(DataSourceConfig config, string apiKey, string secretKey)
        {
            var userId = GetStringParam(config.Parameters, "userId");
            if (string.IsNullOrEmpty(userId))
            {
                _logger.LogWarning("Amplitude: 'userId' parameter not provided for user_activity; returning empty.");
                return new List<object>();
            }

            var url = $"{BaseUrl}/useractivity?user={Uri.EscapeDataString(userId)}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, apiKey, secretKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("userData", out var userData)
                && userData.TryGetProperty("events", out var events)
                && events.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in events.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    foreach (var prop in element.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                    results.Add(row);
                }
            }

            _logger.LogInformation("Amplitude: read {Count} user activity events.", results.Count);
            return results;
        }

        // ── Revenue ──────────────────────────────────────────────────────────

        private async Task<List<object>> ReadRevenueAsync(DataSourceConfig config, string apiKey, string secretKey)
        {
            var startDate = GetStringParam(config.Parameters, "startDate")
                ?? DateTime.UtcNow.AddDays(-30).ToString("yyyyMMdd");
            var endDate = GetStringParam(config.Parameters, "endDate")
                ?? DateTime.UtcNow.ToString("yyyyMMdd");

            var url = $"{BaseUrl}/revenue/day?start={Uri.EscapeDataString(startDate)}&end={Uri.EscapeDataString(endDate)}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, apiKey, secretKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
            {
                FlattenJsonElement(data, results);
            }
            else
            {
                FlattenJsonElement(doc.RootElement, results);
            }

            _logger.LogInformation("Amplitude: read {Count} revenue records.", results.Count);
            return results;
        }

        // ── Generic endpoint read ────────────────────────────────────────────

        private async Task<List<object>> ReadGenericAsync(string apiKey, string secretKey, string path)
        {
            var url = $"{BaseUrl}{path}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyBasicAuth(request, apiKey, secretKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();
            FlattenJsonElement(doc.RootElement, results);

            _logger.LogInformation("Amplitude: read {Count} records from '{Path}'.", results.Count, path);
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
                if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonElement(data, results);
                }
                else if (root.TryGetProperty("results", out var nested) && nested.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonElement(nested, results);
                }
                else
                {
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

        private static void ApplyBasicAuth(HttpRequestMessage request, string apiKey, string secretKey)
        {
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:{secretKey}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // ── Credential resolution ────────────────────────────────────────────

        private (string apiKey, string secretKey) ResolveCredentials(DataSourceConfig config)
        {
            var apiKey = GetStringParam(config.Parameters, "apiKey");
            var secretKey = GetStringParam(config.Parameters, "secretKey");

            // Fall back to ConnectionString JSON: {"apiKey":"...","secretKey":"..."}
            if (string.IsNullOrEmpty(apiKey) || string.IsNullOrEmpty(secretKey))
            {
                if (!string.IsNullOrWhiteSpace(config.ConnectionString))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(config.ConnectionString);
                        if (string.IsNullOrEmpty(apiKey) && doc.RootElement.TryGetProperty("apiKey", out var ak))
                            apiKey = ak.GetString();
                        if (string.IsNullOrEmpty(secretKey) && doc.RootElement.TryGetProperty("secretKey", out var sk))
                            secretKey = sk.GetString();
                    }
                    catch (JsonException)
                    {
                        // ConnectionString is not JSON; treat as plain API key.
                        apiKey ??= config.ConnectionString;
                    }
                }
            }

            if (string.IsNullOrEmpty(apiKey))
                throw new ConnectorException(
                    "Amplitude API key is required. Provide it via Parameters['apiKey'] or the connection string.",
                    new ArgumentException("Missing 'apiKey'."),
                    "amplitude");

            if (string.IsNullOrEmpty(secretKey))
                throw new ConnectorException(
                    "Amplitude secret key is required. Provide it via Parameters['secretKey'] or the connection string.",
                    new ArgumentException("Missing 'secretKey'."),
                    "amplitude");

            return (apiKey, secretKey);
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
                    $"Amplitude connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "amplitude");
            return value;
        }
    }
}
