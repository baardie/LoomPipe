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
    /// Reads data from Harvest using the v2 API.
    ///
    /// Parameters:
    ///   accessToken  — Harvest personal access token or OAuth token
    ///   accountId    — Harvest Account ID (sent as Harvest-Account-Id header)
    ///   resource     — time_entries, projects, clients, tasks, users, invoices,
    ///                  expenses, roles, contacts, estimates
    /// </summary>
    public class HarvestReader : ISourceReader
    {
        private const string BaseUrl = "https://api.harvestapp.com/v2";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "time_entries", "projects", "clients", "tasks", "users",
            "invoices", "expenses", "roles", "contacts", "estimates"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<HarvestReader> _logger;

        public HarvestReader(HttpClient httpClient, ILogger<HarvestReader> logger)
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
                    "Harvest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "harvest");
            var accountId = GetRequiredParam(config.Parameters, "accountId");

            _logger.LogInformation("Harvest: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, accountId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Harvest: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Harvest resource '{resource}': {ex.Message}", ex, "harvest");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Harvest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "harvest");
            var accountId = GetRequiredParam(config.Parameters, "accountId");

            _logger.LogInformation("Harvest: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, accountId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Harvest: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Harvest schema for '{resource}': {ex.Message}", ex, "harvest");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Harvest access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "harvest");
            var accountId = GetRequiredParam(config.Parameters, "accountId");

            _logger.LogInformation("Harvest: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, accountId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Harvest: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Harvest dry run preview failed for '{resource}': {ex.Message}", ex, "harvest");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET with page/per_page, check links.next) ───

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string accountId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageNumber = 1;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/{resource}?page={pageNumber}&per_page={PageSize}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, accountId);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                // Harvest wraps results in a property named after the resource (e.g. "time_entries": [...]).
                if (root.TryGetProperty(resource, out var resourceArray) && resourceArray.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(resourceArray, results);
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(root, results);
                }

                // Check for next page via links.next.
                bool hasNext = false;
                if (root.TryGetProperty("links", out var links)
                    && links.TryGetProperty("next", out var next)
                    && next.ValueKind == JsonValueKind.String
                    && !string.IsNullOrEmpty(next.GetString()))
                {
                    hasNext = true;
                }

                page++;
                pageNumber++;

                if (!hasNext)
                    break;
            }
            while (page < maxPages);

            _logger.LogInformation("Harvest: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Response parsing ────────────────────────────────────────────────

        private static void ParseArray(JsonElement items, List<object> results)
        {
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

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string accountId)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("Harvest-Account-Id", accountId);
            request.Headers.Add("User-Agent", "LoomPipe Connector");
        }

        // ── Parameter helpers ───────────────────────────────────────────────

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
                    $"Harvest connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "harvest");
            return value;
        }
    }
}
