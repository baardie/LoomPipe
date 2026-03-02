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
    /// Reads CRM objects from Copper using the v1 Developer API.
    ///
    /// Parameters:
    ///   accessToken  — Copper API key
    ///   email        — user email associated with the Copper account
    ///   resource     — leads, people, companies, opportunities, tasks, activities,
    ///                  projects, tags, custom_fields, pipelines
    /// </summary>
    public class CopperReader : ISourceReader
    {
        private const string BaseUrl = "https://api.copper.com/developer_api/v1";
        private const int PageSize = 200;

        private static readonly string[] AllResources =
        {
            "leads", "people", "companies", "opportunities", "tasks",
            "activities", "projects", "tags", "custom_fields", "pipelines"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<CopperReader> _logger;

        public CopperReader(HttpClient httpClient, ILogger<CopperReader> logger)
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
                    "Copper API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "copper");
            var email = GetRequiredParam(config.Parameters, "email");

            _logger.LogInformation("Copper: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, email);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Copper: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Copper resource '{resource}': {ex.Message}", ex, "copper");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Copper API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "copper");
            var email = GetRequiredParam(config.Parameters, "email");

            _logger.LogInformation("Copper: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, email, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Copper: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Copper schema for '{resource}': {ex.Message}", ex, "copper");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Copper API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "copper");
            var email = GetRequiredParam(config.Parameters, "email");

            _logger.LogInformation("Copper: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, email, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Copper: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Copper dry run preview failed for '{resource}': {ex.Message}", ex, "copper");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated POST search) ────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string email, int maxPages = int.MaxValue)
        {
            var results = new List<object>();

            // Resources that use a simple GET list instead of POST search.
            if (resource is "tags" or "custom_fields" or "pipelines")
            {
                var url = $"{BaseUrl}/{resource}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResponseArray(doc.RootElement, results);

                _logger.LogInformation("Copper: read {Count} records from '{Resource}'.",
                    results.Count, resource);

                return results;
            }

            // Searchable resources use POST with pagination in body.
            int pageNumber = 1;
            int page = 0;

            while (page < maxPages)
            {
                var url = $"{BaseUrl}/{resource}/search";
                var body = BuildSearchBody(pageNumber);

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResponseArray(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                page++;
                pageNumber++;

                // If fewer than PageSize records returned, we've reached the last page.
                if (fetched < PageSize)
                    break;
            }

            _logger.LogInformation("Copper: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Request body builder ────────────────────────────────────────────

        private static string BuildSearchBody(int pageNumber)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteNumber("page_size", PageSize);
            writer.WriteNumber("page_number", pageNumber);
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // ── Response parsing ────────────────────────────────────────────────

        private static void ParseResponseArray(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else if (root.TryGetProperty("results", out var r) && r.ValueKind == JsonValueKind.Array)
            {
                items = r;
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

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string email)
        {
            request.Headers.Add("X-PW-AccessToken", accessToken);
            request.Headers.Add("X-PW-Application", "developer_api");
            request.Headers.Add("X-PW-UserEmail", email);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
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
                    $"Copper connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "copper");
            return value;
        }
    }
}
