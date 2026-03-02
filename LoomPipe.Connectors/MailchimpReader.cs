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
    /// Reads data from the Mailchimp Marketing API v3.
    ///
    /// Parameters:
    ///   accessToken  — Mailchimp API key (e.g. "xxxxxxx-us21"); the data centre suffix
    ///                  is extracted automatically from the key.
    ///   resource     — lists, campaigns, members, reports, automations, templates,
    ///                  segments, tags, landing_pages
    ///   listId       — required when resource is "members", "segments", or "tags"
    /// </summary>
    public class MailchimpReader : ISourceReader
    {
        private const int PageCount = 100;

        private static readonly string[] AllResources =
        {
            "lists", "campaigns", "members", "reports", "automations",
            "templates", "segments", "tags", "landing_pages"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MailchimpReader> _logger;

        public MailchimpReader(HttpClient httpClient, ILogger<MailchimpReader> logger)
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
            var apiKey     = ResolveApiKey(config);
            var baseUrl    = BuildBaseUrl(apiKey);

            _logger.LogInformation("Mailchimp: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, apiKey, baseUrl, config.Parameters);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mailchimp: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Mailchimp resource '{resource}': {ex.Message}", ex, "mailchimp");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey   = ResolveApiKey(config);
            var baseUrl  = BuildBaseUrl(apiKey);

            _logger.LogInformation("Mailchimp: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, apiKey, baseUrl, config.Parameters, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mailchimp: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Mailchimp schema for '{resource}': {ex.Message}", ex, "mailchimp");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey   = ResolveApiKey(config);
            var baseUrl  = BuildBaseUrl(apiKey);

            _logger.LogInformation("Mailchimp: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, apiKey, baseUrl, config.Parameters, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mailchimp: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Mailchimp dry run preview failed for '{resource}': {ex.Message}", ex, "mailchimp");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET with offset) ────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string apiKey, string baseUrl,
            Dictionary<string, object> parameters, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;
            bool hasMore = true;

            do
            {
                var url = BuildUrl(resource, baseUrl, parameters, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, apiKey);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var resourceKey = GetResourceKey(resource);
                int count = ParseResultsPage(doc.RootElement, resourceKey, results);

                // If we got fewer results than the page size, we've reached the end.
                hasMore = count >= PageCount;
                offset += count;
                page++;
            }
            while (hasMore && page < maxPages);

            _logger.LogInformation("Mailchimp: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string baseUrl, Dictionary<string, object> parameters, int offset)
        {
            var listId = GetStringParam(parameters, "listId");

            var endpoint = resource switch
            {
                "lists"         => "/lists",
                "campaigns"     => "/campaigns",
                "members"       => $"/lists/{RequireListId(listId, resource)}/members",
                "reports"       => "/reports",
                "automations"   => "/automations",
                "templates"     => "/templates",
                "segments"      => $"/lists/{RequireListId(listId, resource)}/segments",
                "tags"          => $"/lists/{RequireListId(listId, resource)}/tag-search",
                "landing_pages" => "/landing-pages",
                _ => throw new ConnectorException(
                    $"Mailchimp: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "mailchimp")
            };

            return $"{baseUrl}{endpoint}?count={PageCount}&offset={offset}";
        }

        private static string RequireListId(string? listId, string resource)
        {
            if (string.IsNullOrWhiteSpace(listId))
                throw new ConnectorException(
                    $"Mailchimp resource '{resource}' requires the 'listId' parameter.",
                    new ArgumentException("Missing 'listId'."),
                    "mailchimp");
            return listId;
        }

        /// <summary>
        /// Returns the JSON property name that wraps the results array for a given resource.
        /// </summary>
        private static string GetResourceKey(string resource) => resource switch
        {
            "lists"         => "lists",
            "campaigns"     => "campaigns",
            "members"       => "members",
            "reports"       => "reports",
            "automations"   => "automations",
            "templates"     => "templates",
            "segments"      => "segments",
            "tags"          => "tags",
            "landing_pages" => "landing_pages",
            _               => resource
        };

        // ── Base URL builder ─────────────────────────────────────────────────

        /// <summary>
        /// Extracts the data centre from the API key suffix (e.g. "us21") and builds the base URL.
        /// </summary>
        private static string BuildBaseUrl(string apiKey)
        {
            var dashIndex = apiKey.LastIndexOf('-');
            if (dashIndex < 0 || dashIndex >= apiKey.Length - 1)
                throw new ConnectorException(
                    "Mailchimp API key must end with a data centre suffix (e.g. '-us21').",
                    new ArgumentException("Invalid Mailchimp API key format."),
                    "mailchimp");

            var dc = apiKey.Substring(dashIndex + 1);
            return $"https://{dc}.api.mailchimp.com/3.0";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static int ParseResultsPage(JsonElement root, string resourceKey, List<object> results)
        {
            JsonElement items;
            int count = 0;

            if (root.TryGetProperty(resourceKey, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard response shape
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return 0;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten nested objects (e.g. "stats", "_links").
                        if (prop.Name == "_links") continue; // Skip HATEOAS links.
                        foreach (var nested in prop.Value.EnumerateObject())
                        {
                            row[$"{prop.Name}_{nested.Name}"] = ConvertJsonValue(nested.Value);
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        if (prop.Name == "_links") continue;
                        row[prop.Name] = prop.Value.ToString();
                    }
                    else
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
                count++;
            }

            return count;
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
            // Mailchimp uses Basic auth with any username and the API key as password.
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"anystring:{apiKey}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // ── API key resolution ───────────────────────────────────────────────

        private string ResolveApiKey(DataSourceConfig config)
        {
            return GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Mailchimp API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "mailchimp");
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
                    $"Mailchimp connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "mailchimp");
            return value;
        }
    }
}
