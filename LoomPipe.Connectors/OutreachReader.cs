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
    /// Reads data from Outreach.io using the v2 JSON:API.
    ///
    /// Parameters:
    ///   accessToken  — OAuth Bearer token
    ///   resource     — prospects, accounts, sequences, mailings, calls, tasks, users,
    ///                  teams, stages, events, templates
    /// </summary>
    public class OutreachReader : ISourceReader
    {
        private const string BaseUrl = "https://api.outreach.io/api/v2";
        private const int PageLimit = 50;

        private static readonly string[] AllResources =
        {
            "prospects", "accounts", "sequences", "mailings", "calls", "tasks",
            "users", "teams", "stages", "events", "templates"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<OutreachReader> _logger;

        public OutreachReader(HttpClient httpClient, ILogger<OutreachReader> logger)
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
                    "Outreach access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "outreach");

            _logger.LogInformation("Outreach: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outreach: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Outreach resource '{resource}': {ex.Message}", ex, "outreach");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Outreach access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "outreach");

            _logger.LogInformation("Outreach: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outreach: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Outreach schema for '{resource}': {ex.Message}", ex, "outreach");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Outreach access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "outreach");

            _logger.LogInformation("Outreach: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outreach: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Outreach dry run preview failed for '{resource}': {ex.Message}", ex, "outreach");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET, JSON:API) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            string? nextUrl = null;

            do
            {
                var url = nextUrl ?? $"{BaseUrl}/{resource}?page%5Bsize%5D={PageLimit}&page%5Boffset%5D={offset}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseJsonApiPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                if (fetched == 0) break;

                // JSON:API pagination: follow links.next if available
                nextUrl = null;
                if (doc.RootElement.TryGetProperty("links", out var links)
                    && links.TryGetProperty("next", out var nextEl)
                    && nextEl.ValueKind == JsonValueKind.String)
                {
                    nextUrl = nextEl.GetString();
                }

                offset += fetched;
                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Outreach: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── JSON:API response parsing ────────────────────────────────────────

        private static void ParseJsonApiPage(JsonElement root, List<object> results)
        {
            if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
                return;

            foreach (var element in data.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                // Top-level JSON:API fields: type, id
                if (element.TryGetProperty("id", out var id))
                    row["id"] = id.GetString() ?? id.ToString();

                if (element.TryGetProperty("type", out var type))
                    row["type"] = type.GetString() ?? type.ToString();

                // Flatten the "attributes" sub-object into the row
                if (element.TryGetProperty("attributes", out var attrs) && attrs.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in attrs.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
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
                    $"Outreach connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "outreach");
            return value;
        }
    }
}
