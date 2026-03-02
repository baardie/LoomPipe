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
    /// Reads data from Workday using the REST API.
    ///
    /// Parameters:
    ///   accessToken  — OAuth Bearer token
    ///   host         — Workday API host (e.g. wd5-impl-services1.workday.com)
    ///   tenant       — Workday tenant name
    ///   resource     — workers, organizations, locations, jobs, compensation, time_off,
    ///                  payroll, benefits, recruiting, learning
    ///
    /// ConnectionString JSON: {"host":"...","tenant":"...","accessToken":"..."}
    /// </summary>
    public class WorkdayReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "workers", "organizations", "locations", "jobs", "compensation",
            "time_off", "payroll", "benefits", "recruiting", "learning"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<WorkdayReader> _logger;

        public WorkdayReader(HttpClient httpClient, ILogger<WorkdayReader> logger)
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
            var (host, tenant, accessToken) = ResolveConnectionParams(config);

            _logger.LogInformation("Workday: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, host, tenant, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Workday: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Workday resource '{resource}': {ex.Message}", ex, "workday");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (host, tenant, accessToken) = ResolveConnectionParams(config);

            _logger.LogInformation("Workday: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, host, tenant, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Workday: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Workday schema for '{resource}': {ex.Message}", ex, "workday");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (host, tenant, accessToken) = ResolveConnectionParams(config);

            _logger.LogInformation("Workday: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, host, tenant, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Workday: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Workday dry run preview failed for '{resource}': {ex.Message}", ex, "workday");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (offset + limit pagination) ────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string host, string tenant, string accessToken,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;
            int total = int.MaxValue;

            do
            {
                var url = $"https://{host}/ccx/api/v1/{tenant}/{resource}?limit={PageLimit}&offset={offset}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                // Extract total count from response if available
                if (doc.RootElement.TryGetProperty("total", out var totalEl)
                    && totalEl.ValueKind == JsonValueKind.Number)
                {
                    total = totalEl.GetInt32();
                }

                if (fetched == 0) break;

                offset += fetched;
                page++;
            }
            while (offset < total && page < maxPages);

            _logger.LogInformation("Workday: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Workday response shape: { "data": [...], "total": N }
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

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
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

        // ── Connection parameter resolution ──────────────────────────────────

        /// <summary>
        /// Resolves host, tenant, and accessToken from Parameters or ConnectionString JSON.
        /// ConnectionString format: {"host":"...","tenant":"...","accessToken":"..."}
        /// </summary>
        private (string host, string tenant, string accessToken) ResolveConnectionParams(DataSourceConfig config)
        {
            var host        = GetStringParam(config.Parameters, "host");
            var tenant      = GetStringParam(config.Parameters, "tenant");
            var accessToken = GetStringParam(config.Parameters, "accessToken");

            // Fall back to parsing ConnectionString as JSON
            if ((string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(tenant) || string.IsNullOrWhiteSpace(accessToken))
                && !string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    var root = doc.RootElement;

                    if (string.IsNullOrWhiteSpace(host)
                        && root.TryGetProperty("host", out var hostEl)
                        && hostEl.ValueKind == JsonValueKind.String)
                    {
                        host = hostEl.GetString();
                    }

                    if (string.IsNullOrWhiteSpace(tenant)
                        && root.TryGetProperty("tenant", out var tenantEl)
                        && tenantEl.ValueKind == JsonValueKind.String)
                    {
                        tenant = tenantEl.GetString();
                    }

                    if (string.IsNullOrWhiteSpace(accessToken)
                        && root.TryGetProperty("accessToken", out var tokenEl)
                        && tokenEl.ValueKind == JsonValueKind.String)
                    {
                        accessToken = tokenEl.GetString();
                    }
                }
                catch (JsonException)
                {
                    // ConnectionString is not valid JSON — ignore and rely on Parameters
                }
            }

            if (string.IsNullOrWhiteSpace(host))
                throw new ConnectorException(
                    "Workday host is required. Provide it via Parameters['host'] or the connection string JSON.",
                    new ArgumentException("Missing 'host'."),
                    "workday");

            if (string.IsNullOrWhiteSpace(tenant))
                throw new ConnectorException(
                    "Workday tenant is required. Provide it via Parameters['tenant'] or the connection string JSON.",
                    new ArgumentException("Missing 'tenant'."),
                    "workday");

            if (string.IsNullOrWhiteSpace(accessToken))
                throw new ConnectorException(
                    "Workday access token is required. Provide it via Parameters['accessToken'] or the connection string JSON.",
                    new ArgumentException("Missing 'accessToken'."),
                    "workday");

            return (host!, tenant!, accessToken!);
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
                    $"Workday connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "workday");
            return value;
        }
    }
}
