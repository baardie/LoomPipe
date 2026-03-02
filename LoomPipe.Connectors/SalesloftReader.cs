#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from Salesloft using the v2 API.
    ///
    /// Parameters:
    ///   accessToken  — Salesloft API key or OAuth token
    ///   resource     — people, accounts, cadences, cadence_memberships, activities, emails,
    ///                  calls, tasks, steps, users, teams, notes, imports
    /// </summary>
    public class SalesloftReader : ISourceReader
    {
        private const string BaseUrl = "https://api.salesloft.com/v2";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "people", "accounts", "cadences", "cadence_memberships", "activities",
            "emails", "calls", "tasks", "steps", "users", "teams", "notes", "imports"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SalesloftReader> _logger;

        public SalesloftReader(HttpClient httpClient, ILogger<SalesloftReader> logger)
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
                    "Salesloft access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "salesloft");

            _logger.LogInformation("Salesloft: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesloft: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Salesloft resource '{resource}': {ex.Message}", ex, "salesloft");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Salesloft access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "salesloft");

            _logger.LogInformation("Salesloft: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesloft: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Salesloft schema for '{resource}': {ex.Message}", ex, "salesloft");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Salesloft access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "salesloft");

            _logger.LogInformation("Salesloft: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Salesloft: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Salesloft dry run preview failed for '{resource}': {ex.Message}", ex, "salesloft");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (page-based pagination) ────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int currentPage = 1;
            int totalPages = int.MaxValue;
            int pageCount = 0;

            var endpoint = NormalizeEndpoint(resource);

            do
            {
                var url = $"{BaseUrl}/{endpoint}?per_page={PageLimit}&page={currentPage}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse the "data" array
                if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in data.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }

                // Parse pagination metadata
                if (root.TryGetProperty("metadata", out var metadata)
                    && metadata.TryGetProperty("paging", out var paging))
                {
                    if (paging.TryGetProperty("total_pages", out var tp) && tp.ValueKind == JsonValueKind.Number)
                        totalPages = tp.GetInt32();
                }

                currentPage++;
                pageCount++;
            }
            while (currentPage <= totalPages && pageCount < maxPages);

            _logger.LogInformation("Salesloft: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pageCount);

            return results;
        }

        // ── Endpoint normalisation ──────────────────────────────────────────

        /// <summary>
        /// Normalises the resource name into the Salesloft API endpoint path segment.
        /// Most resources use their plural name directly as the endpoint.
        /// </summary>
        private static string NormalizeEndpoint(string resource) => resource switch
        {
            "activities"          => "activities/calls",   // Default to call activities; other activity types are separate endpoints
            "cadence_memberships" => "cadence_memberships",
            _                     => resource
        };

        // ── JSON flattening ─────────────────────────────────────────────────

        private static object FlattenJsonObject(JsonElement element)
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

        // ── Auth helper ─────────────────────────────────────────────────────

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
                    $"Salesloft connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "salesloft");
            return value;
        }
    }
}
