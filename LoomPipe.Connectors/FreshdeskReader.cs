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
    /// Reads records from Freshdesk using the v2 REST API.
    ///
    /// Parameters:
    ///   accessToken  — Freshdesk API key (used as Basic auth username with "X" as password)
    ///   domain       — Freshdesk subdomain (e.g. "mycompany" for mycompany.freshdesk.com)
    ///   resource     — tickets, contacts, companies, agents, groups, conversations,
    ///                  products, email_configs, forums, solutions, time_entries, satisfaction_ratings
    /// </summary>
    public class FreshdeskReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "tickets", "contacts", "companies", "agents", "groups", "conversations",
            "products", "email_configs", "forums", "solutions", "time_entries",
            "satisfaction_ratings"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<FreshdeskReader> _logger;

        public FreshdeskReader(HttpClient httpClient, ILogger<FreshdeskReader> logger)
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
            var resource   = GetRequiredParam(config.Parameters, "resource");
            var domain     = GetRequiredParam(config.Parameters, "domain");
            var apiKey     = ResolveApiKey(config);

            _logger.LogInformation("Freshdesk: reading resource '{Resource}' from domain '{Domain}'.", resource, domain);

            try
            {
                return await ReadPaginatedAsync(domain, apiKey, resource, watermarkField, watermarkValue);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshdesk: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Freshdesk resource '{resource}': {ex.Message}", ex, "freshdesk");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var domain   = GetRequiredParam(config.Parameters, "domain");
            var apiKey   = ResolveApiKey(config);

            _logger.LogInformation("Freshdesk: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadPaginatedAsync(domain, apiKey, resource, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshdesk: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Freshdesk schema for '{resource}': {ex.Message}", ex, "freshdesk");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var domain   = GetRequiredParam(config.Parameters, "domain");
            var apiKey   = ResolveApiKey(config);

            _logger.LogInformation("Freshdesk: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(domain, apiKey, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Freshdesk: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Freshdesk dry run preview failed for '{resource}': {ex.Message}", ex, "freshdesk");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string domain, string apiKey, string resource,
            string? watermarkField = null, string? watermarkValue = null,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;
            int pageCount = 0;

            do
            {
                var url = BuildUrl(domain, resource, page, watermarkField, watermarkValue);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, apiKey);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                pageCount++;
                page++;

                // Freshdesk: if fewer results than per_page, we've reached the last page.
                if (fetched < PageLimit) break;
            }
            while (pageCount < maxPages);

            _logger.LogInformation("Freshdesk: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, pageCount);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string domain, string resource, int page,
            string? watermarkField = null, string? watermarkValue = null)
        {
            var sb = new StringBuilder($"https://{domain}.freshdesk.com/api/v2/{resource}");
            sb.Append($"?per_page={PageLimit}&page={page}");

            // Freshdesk supports updated_since filter for tickets.
            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue)
                && resource == "tickets")
            {
                sb.Append($"&updated_since={Uri.EscapeDataString(watermarkValue)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string apiKey)
        {
            // Freshdesk uses Basic auth: API key as username, "X" as password.
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:X"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", encoded);
        }

        private string ResolveApiKey(DataSourceConfig config)
        {
            return GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Freshdesk API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "freshdesk");
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
                    $"Freshdesk connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "freshdesk");
            return value;
        }
    }
}
