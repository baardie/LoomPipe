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
    /// Reads observability data from Datadog using its v1/v2 APIs.
    ///
    /// Parameters:
    ///   apiKey     — Datadog API key (DD-API-KEY header)
    ///   appKey     — Datadog application key (DD-APPLICATION-KEY header)
    ///   resource   — metrics, events, monitors, dashboards, logs, hosts,
    ///                incidents, service_level_objectives, synthetics, downtimes
    ///   startDate  — optional ISO date for time-ranged queries (events, logs)
    ///   endDate    — optional ISO date for time-ranged queries
    ///   site       — optional Datadog site (default: datadoghq.com; use datadoghq.eu for EU)
    ///
    /// ConnectionString JSON: {"apiKey":"...","appKey":"...","site":"datadoghq.com"}
    /// </summary>
    public class DatadogReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "metrics", "events", "monitors", "dashboards", "logs", "hosts",
            "incidents", "service_level_objectives", "synthetics", "downtimes"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<DatadogReader> _logger;

        public DatadogReader(HttpClient httpClient, ILogger<DatadogReader> logger)
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
            var (apiKey, appKey, site) = ResolveCredentials(config);
            var startDate = GetStringParam(config.Parameters, "startDate");
            var endDate   = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("Datadog: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, apiKey, appKey, site, startDate, endDate);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Datadog: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Datadog resource '{resource}': {ex.Message}", ex, "datadog");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (apiKey, appKey, site) = ResolveCredentials(config);
            var startDate = GetStringParam(config.Parameters, "startDate");
            var endDate   = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("Datadog: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, apiKey, appKey, site, startDate, endDate, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Datadog: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Datadog schema for '{resource}': {ex.Message}", ex, "datadog");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (apiKey, appKey, site) = ResolveCredentials(config);
            var startDate = GetStringParam(config.Parameters, "startDate");
            var endDate   = GetStringParam(config.Parameters, "endDate");

            _logger.LogInformation("Datadog: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, apiKey, appKey, site, startDate, endDate, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Datadog: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Datadog dry run preview failed for '{resource}': {ex.Message}", ex, "datadog");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read ────────────────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string apiKey, string appKey, string site,
            string? startDate, string? endDate, int maxPages = int.MaxValue)
        {
            var baseApiUrl = $"https://api.{site}/api";

            return resource switch
            {
                "events"  => await ReadEventsAsync(baseApiUrl, apiKey, appKey, startDate, endDate, maxPages),
                "logs"    => await ReadLogsAsync(baseApiUrl, apiKey, appKey, startDate, endDate, maxPages),
                _         => await ReadGenericAsync(baseApiUrl, resource, apiKey, appKey, maxPages)
            };
        }

        // ── Events (GET v1) ──────────────────────────────────────────────────

        private async Task<List<object>> ReadEventsAsync(
            string baseApiUrl, string apiKey, string appKey,
            string? startDate, string? endDate, int maxPages)
        {
            var results = new List<object>();

            var startTs = ToUnixTimestamp(startDate, DateTimeOffset.UtcNow.AddDays(-1));
            var endTs   = ToUnixTimestamp(endDate, DateTimeOffset.UtcNow);

            var url = $"{baseApiUrl}/v1/events?start={startTs}&end={endTs}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, apiKey, appKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("events", out var events)
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

            _logger.LogInformation("Datadog: read {Count} events.", results.Count);
            return results;
        }

        // ── Logs (POST v2) ──────────────────────────────────────────────────

        private async Task<List<object>> ReadLogsAsync(
            string baseApiUrl, string apiKey, string appKey,
            string? startDate, string? endDate, int maxPages)
        {
            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            var fromDate = startDate ?? DateTimeOffset.UtcNow.AddHours(-1).ToString("o");
            var toDate   = endDate ?? DateTimeOffset.UtcNow.ToString("o");

            do
            {
                var body = BuildLogsSearchBody(fromDate, toDate, cursor);
                var url = $"{baseApiUrl}/v2/logs/events/search";

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, apiKey, appKey);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int count = 0;
                if (doc.RootElement.TryGetProperty("data", out var data)
                    && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in data.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        if (element.TryGetProperty("id", out var id))
                            row["id"] = id.GetString() ?? id.ToString();

                        if (element.TryGetProperty("attributes", out var attrs)
                            && attrs.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in attrs.EnumerateObject())
                            {
                                row[prop.Name] = ConvertJsonValue(prop.Value);
                            }
                        }

                        results.Add(row);
                        count++;
                    }
                }

                // Pagination: check for cursor in meta.page.after
                cursor = null;
                if (doc.RootElement.TryGetProperty("meta", out var meta)
                    && meta.TryGetProperty("page", out var pageMeta)
                    && pageMeta.TryGetProperty("after", out var afterEl)
                    && afterEl.ValueKind == JsonValueKind.String)
                {
                    cursor = afterEl.GetString();
                }

                if (count == 0)
                    break;

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Datadog: read {Count} log entries across {Pages} page(s).",
                results.Count, page + 1);

            return results;
        }

        // ── Generic resources (GET) ──────────────────────────────────────────

        private async Task<List<object>> ReadGenericAsync(
            string baseApiUrl, string resource, string apiKey, string appKey, int maxPages)
        {
            var results = new List<object>();

            var (apiVersion, endpoint, dataKey) = GetResourceEndpoint(resource);
            var url = $"{baseApiUrl}/{apiVersion}/{endpoint}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, apiKey, appKey);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            JsonElement items;
            if (!string.IsNullOrEmpty(dataKey)
                && doc.RootElement.TryGetProperty(dataKey, out items)
                && items.ValueKind == JsonValueKind.Array)
            {
                // standard response with named data array
            }
            else if (doc.RootElement.TryGetProperty("data", out items)
                     && items.ValueKind == JsonValueKind.Array)
            {
                // v2-style "data" array
            }
            else if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                items = doc.RootElement;
            }
            else
            {
                _logger.LogInformation("Datadog: read 0 records from '{Resource}'.", resource);
                return results;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                // For v2 resources, flatten attributes sub-object
                if (element.TryGetProperty("id", out var id))
                    row["id"] = id.GetString() ?? id.ToString();

                if (element.TryGetProperty("attributes", out var attrs)
                    && attrs.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in attrs.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }
                else
                {
                    // v1 resources: flatten all top-level properties
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

            _logger.LogInformation("Datadog: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── Resource endpoint mapping ────────────────────────────────────────

        private static (string apiVersion, string endpoint, string dataKey) GetResourceEndpoint(string resource)
        {
            return resource switch
            {
                "metrics"                    => ("v1", "metrics",                     "metrics"),
                "monitors"                   => ("v1", "monitor",                     string.Empty),
                "dashboards"                 => ("v1", "dashboard",                   "dashboards"),
                "hosts"                      => ("v1", "hosts",                       "host_list"),
                "downtimes"                  => ("v1", "downtime",                    string.Empty),
                "incidents"                  => ("v2", "incidents",                   "data"),
                "service_level_objectives"   => ("v1", "slo",                         "data"),
                "synthetics"                 => ("v1", "synthetics/tests",            "tests"),
                _                            => ("v1", resource,                      string.Empty)
            };
        }

        // ── Body builders ────────────────────────────────────────────────────

        private static string BuildLogsSearchBody(string from, string to, string? cursor)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();

            writer.WritePropertyName("filter");
            writer.WriteStartObject();
            writer.WriteString("from", from);
            writer.WriteString("to", to);
            writer.WriteEndObject();

            writer.WritePropertyName("page");
            writer.WriteStartObject();
            writer.WriteNumber("limit", PageLimit);
            if (!string.IsNullOrEmpty(cursor))
            {
                writer.WriteString("cursor", cursor);
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
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

        private static void ApplyAuth(HttpRequestMessage request, string apiKey, string appKey)
        {
            request.Headers.TryAddWithoutValidation("DD-API-KEY", apiKey);
            request.Headers.TryAddWithoutValidation("DD-APPLICATION-KEY", appKey);
        }

        // ── Credential resolution ────────────────────────────────────────────

        /// <summary>
        /// Resolves apiKey, appKey, and site from Parameters and/or ConnectionString JSON.
        /// ConnectionString JSON format: {"apiKey":"...","appKey":"...","site":"datadoghq.com"}
        /// </summary>
        private (string apiKey, string appKey, string site) ResolveCredentials(DataSourceConfig config)
        {
            var apiKey = GetStringParam(config.Parameters, "apiKey");
            var appKey = GetStringParam(config.Parameters, "appKey");
            var site   = GetStringParam(config.Parameters, "site");

            // Merge from ConnectionString JSON if available
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    var root = doc.RootElement;

                    if (string.IsNullOrEmpty(apiKey) && root.TryGetProperty("apiKey", out var ak)
                        && ak.ValueKind == JsonValueKind.String)
                        apiKey = ak.GetString();

                    if (string.IsNullOrEmpty(appKey) && root.TryGetProperty("appKey", out var apk)
                        && apk.ValueKind == JsonValueKind.String)
                        appKey = apk.GetString();

                    if (string.IsNullOrEmpty(site) && root.TryGetProperty("site", out var s)
                        && s.ValueKind == JsonValueKind.String)
                        site = s.GetString();
                }
                catch (JsonException)
                {
                    // ConnectionString is not JSON — ignore
                }
            }

            if (string.IsNullOrWhiteSpace(apiKey))
                throw new ConnectorException(
                    "Datadog API key is required. Provide it via Parameters['apiKey'] or the connection string JSON.",
                    new ArgumentException("Missing 'apiKey'."),
                    "datadog");

            if (string.IsNullOrWhiteSpace(appKey))
                throw new ConnectorException(
                    "Datadog application key is required. Provide it via Parameters['appKey'] or the connection string JSON.",
                    new ArgumentException("Missing 'appKey'."),
                    "datadog");

            site = string.IsNullOrWhiteSpace(site) ? "datadoghq.com" : site;

            return (apiKey, appKey, site);
        }

        // ── Timestamp helper ─────────────────────────────────────────────────

        private static long ToUnixTimestamp(string? isoDate, DateTimeOffset fallback)
        {
            if (!string.IsNullOrEmpty(isoDate) && DateTimeOffset.TryParse(isoDate, out var parsed))
                return parsed.ToUnixTimeSeconds();
            return fallback.ToUnixTimeSeconds();
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
                    $"Datadog connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "datadog");
            return value;
        }
    }
}
