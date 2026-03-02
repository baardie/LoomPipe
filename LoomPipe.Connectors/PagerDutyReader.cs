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
    /// Reads incident and operations data from PagerDuty using the v2 REST API.
    ///
    /// Parameters:
    ///   accessToken  — PagerDuty API key or token (Authorization: Token token={apiKey})
    ///   resource     — incidents, services, users, teams, schedules, escalation_policies,
    ///                  on_calls, log_entries, priorities, vendors, maintenance_windows
    /// </summary>
    public class PagerDutyReader : ISourceReader
    {
        private const string BaseUrl = "https://api.pagerduty.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "incidents", "services", "users", "teams", "schedules",
            "escalation_policies", "on_calls", "log_entries",
            "priorities", "vendors", "maintenance_windows"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<PagerDutyReader> _logger;

        public PagerDutyReader(HttpClient httpClient, ILogger<PagerDutyReader> logger)
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
            var apiKey   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "PagerDuty API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pagerduty");

            _logger.LogInformation("PagerDuty: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, apiKey);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PagerDuty: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read PagerDuty resource '{resource}': {ex.Message}", ex, "pagerduty");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "PagerDuty API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pagerduty");

            _logger.LogInformation("PagerDuty: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, apiKey, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PagerDuty: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover PagerDuty schema for '{resource}': {ex.Message}", ex, "pagerduty");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "PagerDuty API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pagerduty");

            _logger.LogInformation("PagerDuty: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, apiKey, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PagerDuty: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"PagerDuty dry run preview failed for '{resource}': {ex.Message}", ex, "pagerduty");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string apiKey, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, apiKey);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var dataKey = GetDataKey(resource);
                int count = 0;

                if (doc.RootElement.TryGetProperty(dataKey, out var data)
                    && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in data.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                        count++;
                    }
                }

                // Pagination: check "more" boolean
                bool hasMore = false;
                if (doc.RootElement.TryGetProperty("more", out var moreEl)
                    && moreEl.ValueKind == JsonValueKind.True)
                {
                    hasMore = true;
                }

                if (!hasMore || count == 0)
                    break;

                offset += count;
                page++;
            }
            while (page < maxPages);

            _logger.LogInformation("PagerDuty: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page + 1);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, int offset)
        {
            var endpoint = NormalizeEndpoint(resource);

            var sb = new StringBuilder($"{BaseUrl}/{endpoint}");
            sb.Append($"?limit={PageLimit}");
            sb.Append($"&offset={offset}");
            sb.Append("&total=true");

            return sb.ToString();
        }

        /// <summary>
        /// Normalises the resource name into the PagerDuty API endpoint path segment.
        /// </summary>
        private static string NormalizeEndpoint(string resource) => resource switch
        {
            "on_calls"            => "oncalls",
            "maintenance_windows" => "maintenance_windows",
            _                     => resource
        };

        /// <summary>
        /// Returns the JSON property name containing the array of items for a given resource.
        /// </summary>
        private static string GetDataKey(string resource) => resource switch
        {
            "incidents"            => "incidents",
            "services"             => "services",
            "users"                => "users",
            "teams"                => "teams",
            "schedules"            => "schedules",
            "escalation_policies"  => "escalation_policies",
            "on_calls"             => "oncalls",
            "log_entries"          => "log_entries",
            "priorities"           => "priorities",
            "vendors"              => "vendors",
            "maintenance_windows"  => "maintenance_windows",
            _                      => resource
        };

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

        private static void ApplyAuth(HttpRequestMessage request, string apiKey)
        {
            // PagerDuty uses "Authorization: Token token=<apiKey>"
            request.Headers.TryAddWithoutValidation("Authorization", $"Token token={apiKey}");
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
                    $"PagerDuty connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "pagerduty");
            return value;
        }
    }
}
