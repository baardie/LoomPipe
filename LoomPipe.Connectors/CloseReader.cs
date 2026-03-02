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
    /// Reads CRM objects from Close using the v1 API.
    ///
    /// Parameters:
    ///   accessToken  — Close API key (used as Basic auth username)
    ///   resource     — leads, contacts, opportunities, activities, tasks, users,
    ///                  pipelines, statuses, smart_views, custom_fields, bulk_actions
    ///   query        — optional search query string for lead search
    /// </summary>
    public class CloseReader : ISourceReader
    {
        private const string BaseUrl = "https://api.close.com/api/v1";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "leads", "contacts", "opportunities", "activities", "tasks",
            "users", "pipelines", "statuses", "smart_views", "custom_fields",
            "bulk_actions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<CloseReader> _logger;

        public CloseReader(HttpClient httpClient, ILogger<CloseReader> logger)
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
                    "Close API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "close");
            var query = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Close: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, query);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Close: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Close resource '{resource}': {ex.Message}", ex, "close");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Close API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "close");

            _logger.LogInformation("Close: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, query: null, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Close: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Close schema for '{resource}': {ex.Message}", ex, "close");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Close API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "close");

            _logger.LogInformation("Close: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, query: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Close: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Close dry run preview failed for '{resource}': {ex.Message}", ex, "close");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET with _skip/_limit) ──────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? query, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var endpoint = MapResourceEndpoint(resource);
            int skip = 0;
            int page = 0;

            do
            {
                var url = BuildUrl(endpoint, query, skip);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                // Close wraps paginated results in { "data": [...], "has_more": bool }
                // Some endpoints return a plain array.
                bool hasMore = false;

                if (root.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(dataArray, results);

                    if (root.TryGetProperty("has_more", out var hm) && hm.ValueKind == JsonValueKind.True)
                        hasMore = true;
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(root, results);
                }
                else
                {
                    // Single object responses (shouldn't happen for list endpoints).
                    break;
                }

                skip += PageLimit;
                page++;

                if (!hasMore)
                    break;
            }
            while (page < maxPages);

            _logger.LogInformation("Close: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string endpoint, string? query, int skip)
        {
            var sb = new StringBuilder($"{BaseUrl}/{endpoint}/?_limit={PageLimit}&_skip={skip}");

            if (!string.IsNullOrEmpty(query))
            {
                sb.Append($"&query={Uri.EscapeDataString(query)}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Maps a resource name to its Close API endpoint path segment.
        /// </summary>
        private static string MapResourceEndpoint(string resource) => resource switch
        {
            "leads"         => "lead",
            "contacts"      => "contact",
            "opportunities" => "opportunity",
            "activities"    => "activity",
            "tasks"         => "task",
            "users"         => "user",
            "pipelines"     => "pipeline",
            "statuses"      => "status",
            "smart_views"   => "saved_search",
            "custom_fields" => "custom_field",
            "bulk_actions"  => "bulk_action",
            _               => resource
        };

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
            // Close uses Basic auth with the API key as the username and an empty password.
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
                    $"Close connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "close");
            return value;
        }
    }
}
