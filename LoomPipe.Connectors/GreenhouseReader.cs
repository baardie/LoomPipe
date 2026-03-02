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
    /// Reads data from Greenhouse using the Harvest API v1.
    ///
    /// Parameters:
    ///   accessToken  — Greenhouse Harvest API key (used as Basic auth username)
    ///   resource     — candidates, applications, jobs, offers, departments, offices,
    ///                  users, scorecards, scheduled_interviews, job_stages, sources,
    ///                  rejection_reasons
    /// </summary>
    public class GreenhouseReader : ISourceReader
    {
        private const string BaseUrl = "https://harvest.greenhouse.io/v1";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "candidates", "applications", "jobs", "offers", "departments",
            "offices", "users", "scorecards", "scheduled_interviews",
            "job_stages", "sources", "rejection_reasons"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GreenhouseReader> _logger;

        public GreenhouseReader(HttpClient httpClient, ILogger<GreenhouseReader> logger)
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
                    "Greenhouse API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "greenhouse");

            _logger.LogInformation("Greenhouse: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Greenhouse: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Greenhouse resource '{resource}': {ex.Message}", ex, "greenhouse");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Greenhouse API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "greenhouse");

            _logger.LogInformation("Greenhouse: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Greenhouse: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Greenhouse schema for '{resource}': {ex.Message}", ex, "greenhouse");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Greenhouse API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "greenhouse");

            _logger.LogInformation("Greenhouse: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Greenhouse: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Greenhouse dry run preview failed for '{resource}': {ex.Message}", ex, "greenhouse");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET, check Link header for rel="next") ──────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageNumber = 1;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/{resource}?per_page={PageSize}&page={pageNumber}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;
                int countBefore = results.Count;

                if (root.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(root, results);
                }
                else if (root.TryGetProperty("results", out var resultsArray) && resultsArray.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(resultsArray, results);
                }

                int fetched = results.Count - countBefore;
                page++;
                pageNumber++;

                // Check for next page via Link header (rel="next").
                bool hasNext = HasNextLinkHeader(response);

                // Also stop if fewer than PageSize records returned.
                if (!hasNext || fetched < PageSize)
                    break;
            }
            while (page < maxPages);

            _logger.LogInformation("Greenhouse: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Link header parsing ─────────────────────────────────────────────

        /// <summary>
        /// Checks the HTTP response for a Link header containing rel="next".
        /// </summary>
        private static bool HasNextLinkHeader(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return false;

            foreach (var linkHeader in linkValues)
            {
                if (linkHeader.Contains("rel=\"next\"", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            // Greenhouse Harvest API uses Basic auth with the API key as username and empty password.
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{accessToken}:"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
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
                    $"Greenhouse connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "greenhouse");
            return value;
        }
    }
}
