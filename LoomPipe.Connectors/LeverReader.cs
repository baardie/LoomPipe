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
    /// Reads recruiting data from Lever using the v1 API.
    ///
    /// Parameters:
    ///   accessToken  — Lever API key (used as Basic auth username)
    ///   resource     — opportunities, candidates, postings, interviews, offers,
    ///                  users, stages, sources, archive_reasons, tags, requisitions
    /// </summary>
    public class LeverReader : ISourceReader
    {
        private const string BaseUrl = "https://api.lever.co/v1";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "opportunities", "candidates", "postings", "interviews", "offers",
            "users", "stages", "sources", "archive_reasons", "tags", "requisitions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<LeverReader> _logger;

        public LeverReader(HttpClient httpClient, ILogger<LeverReader> logger)
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
            var apiKey = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Lever API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "lever");

            _logger.LogInformation("Lever: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, apiKey);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lever: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Lever resource '{resource}': {ex.Message}", ex, "lever");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Lever API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "lever");

            _logger.LogInformation("Lever: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, apiKey, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lever: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Lever schema for '{resource}': {ex.Message}", ex, "lever");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Lever API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "lever");

            _logger.LogInformation("Lever: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, apiKey, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lever: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Lever dry run preview failed for '{resource}': {ex.Message}", ex, "lever");
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
            string? offset = null;
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

                // Parse data array
                if (doc.RootElement.TryGetProperty("data", out var data)
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
                    }
                }

                // Pagination: check hasNext and next offset token
                offset = null;
                if (doc.RootElement.TryGetProperty("hasNext", out var hasNext)
                    && hasNext.ValueKind == JsonValueKind.True
                    && doc.RootElement.TryGetProperty("next", out var next)
                    && next.ValueKind == JsonValueKind.String)
                {
                    offset = next.GetString();
                }

                page++;
            }
            while (offset != null && page < maxPages);

            _logger.LogInformation("Lever: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? offset)
        {
            var sb = new StringBuilder($"{BaseUrl}/{resource}");
            sb.Append($"?limit={PageLimit}");

            if (!string.IsNullOrEmpty(offset))
            {
                sb.Append($"&offset={Uri.EscapeDataString(offset)}");
            }

            return sb.ToString();
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

        private static void ApplyAuth(HttpRequestMessage request, string apiKey)
        {
            // Lever uses Basic auth with the API key as the username and empty password.
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:"));
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
                    $"Lever connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "lever");
            return value;
        }
    }
}
