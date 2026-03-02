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
    /// Reads CRM objects from Freshsales (Freshworks CRM) using the sales API.
    ///
    /// Parameters:
    ///   accessToken  — Freshsales API key
    ///   domain       — Freshworks subdomain (e.g. "acme" for acme.myfreshworks.com)
    ///   resource     — contacts, accounts, deals, tasks, appointments, notes,
    ///                  sales_activities, products, documents, territories
    /// </summary>
    public class FreshsalesReader : ISourceReader
    {
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "contacts", "accounts", "deals", "tasks", "appointments",
            "notes", "sales_activities", "products", "documents", "territories"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<FreshsalesReader> _logger;

        public FreshsalesReader(HttpClient httpClient, ILogger<FreshsalesReader> logger)
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
                    "Freshsales API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "freshsales");
            var domain = GetRequiredParam(config.Parameters, "domain");

            _logger.LogInformation("Freshsales: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, domain);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshsales: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Freshsales resource '{resource}': {ex.Message}", ex, "freshsales");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Freshsales API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "freshsales");
            var domain = GetRequiredParam(config.Parameters, "domain");

            _logger.LogInformation("Freshsales: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, domain, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshsales: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Freshsales schema for '{resource}': {ex.Message}", ex, "freshsales");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Freshsales API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "freshsales");
            var domain = GetRequiredParam(config.Parameters, "domain");

            _logger.LogInformation("Freshsales: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, domain, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshsales: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Freshsales dry run preview failed for '{resource}': {ex.Message}", ex, "freshsales");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET with page/per_page) ─────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string domain, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var baseUrl = $"https://{domain}.myfreshworks.com/crm/sales/api";
            int pageNumber = 1;
            int page = 0;

            do
            {
                var url = $"{baseUrl}/{resource}?page={pageNumber}&per_page={PageSize}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;
                int countBefore = results.Count;

                // Freshsales wraps results in a property named after the resource (e.g. "contacts": [...]).
                // Some endpoints may also return a plain array or an object with a different wrapper.
                if (root.TryGetProperty(resource, out var resourceArray) && resourceArray.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(resourceArray, results);
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(root, results);
                }
                else
                {
                    // Try the singular form of the resource name as a fallback.
                    var singular = resource.TrimEnd('s');
                    if (root.TryGetProperty(singular, out var singularArray) && singularArray.ValueKind == JsonValueKind.Array)
                    {
                        ParseArray(singularArray, results);
                    }
                }

                int fetched = results.Count - countBefore;
                page++;
                pageNumber++;

                // If fewer than PageSize records returned, we've reached the last page.
                if (fetched < PageSize)
                    break;
            }
            while (page < maxPages);

            _logger.LogInformation("Freshsales: read {Count} records from '{Resource}' across {Pages} page(s).",
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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Token", $"token={accessToken}");
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
                    $"Freshsales connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "freshsales");
            return value;
        }
    }
}
