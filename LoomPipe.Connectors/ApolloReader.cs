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
    /// Reads data from Apollo.io using the v1 API.
    ///
    /// Parameters:
    ///   accessToken  — Apollo API key
    ///   resource     — contacts, accounts, opportunities, activities, tasks, sequences,
    ///                  email_accounts, lists
    ///   query        — optional search term for search endpoints
    /// </summary>
    public class ApolloReader : ISourceReader
    {
        private const string BaseUrl = "https://api.apollo.io/v1";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "contacts", "accounts", "opportunities", "activities", "tasks",
            "sequences", "email_accounts", "lists"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ApolloReader> _logger;

        public ApolloReader(HttpClient httpClient, ILogger<ApolloReader> logger)
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
                    "Apollo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "apollo");
            var query       = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Apollo: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, query);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Apollo: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Apollo resource '{resource}': {ex.Message}", ex, "apollo");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Apollo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "apollo");
            var query       = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Apollo: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, query, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Apollo: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Apollo schema for '{resource}': {ex.Message}", ex, "apollo");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Apollo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "apollo");
            var query       = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Apollo: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, query, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Apollo: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Apollo dry run preview failed for '{resource}': {ex.Message}", ex, "apollo");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? query, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageNum = 1;
            int pagesRead = 0;

            // Resources that use the search (POST) endpoint.
            bool useSearch = resource is "contacts" or "accounts";

            do
            {
                int countBefore = results.Count;

                if (useSearch)
                {
                    await ReadSearchPageAsync(resource, accessToken, query, pageNum, results);
                }
                else
                {
                    await ReadGetPageAsync(resource, accessToken, pageNum, results);
                }

                int fetched = results.Count - countBefore;
                if (fetched == 0) break;

                pageNum++;
                pagesRead++;
            }
            while (pagesRead < maxPages);

            _logger.LogInformation("Apollo: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pagesRead);

            return results;
        }

        // ── Search (POST) for contacts/accounts ─────────────────────────────

        private async Task ReadSearchPageAsync(
            string resource, string accessToken, string? query, int page, List<object> results)
        {
            var url = $"{BaseUrl}/{resource}/search";

            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteNumber("page", page);
            writer.WriteNumber("per_page", PageLimit);
            if (!string.IsNullOrEmpty(query))
            {
                writer.WriteString("q_keywords", query);
            }
            writer.WriteEndObject();
            writer.Flush();

            var body = Encoding.UTF8.GetString(ms.ToArray());

            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            ParseResultsPage(doc.RootElement, resource, results);
        }

        // ── GET for other resources ──────────────────────────────────────────

        private async Task ReadGetPageAsync(
            string resource, string accessToken, int page, List<object> results)
        {
            var url = $"{BaseUrl}/{resource}?page={page}&per_page={PageLimit}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            ParseResultsPage(doc.RootElement, resource, results);
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty(resource, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Apollo wraps results in a key matching the resource name
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
            request.Headers.Add("X-Api-Key", accessToken);
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
                    $"Apollo connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "apollo");
            return value;
        }
    }
}
