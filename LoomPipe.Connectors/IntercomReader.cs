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
    /// Reads data from Intercom using the v2 API.
    ///
    /// Parameters:
    ///   accessToken — Intercom OAuth or personal access token
    ///   resource    — contacts, companies, conversations, admins, teams, tags, segments,
    ///                 data_attributes, articles, collections, help_centers
    /// </summary>
    public class IntercomReader : ISourceReader
    {
        private const string BaseUrl = "https://api.intercom.io";

        private static readonly string[] AllResources =
        {
            "contacts", "companies", "conversations", "admins", "teams", "tags",
            "segments", "data_attributes", "articles", "collections", "help_centers"
        };

        /// <summary>
        /// Per-page limits vary by resource; Intercom uses different defaults.
        /// </summary>
        private static readonly Dictionary<string, int> PageSizes = new()
        {
            ["contacts"]      = 150,
            ["companies"]     = 50,
            ["conversations"] = 20,
            ["articles"]      = 50,
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<IntercomReader> _logger;

        public IntercomReader(HttpClient httpClient, ILogger<IntercomReader> logger)
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
                    "Intercom access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "intercom");

            _logger.LogInformation("Intercom: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Intercom: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Intercom resource '{resource}': {ex.Message}", ex, "intercom");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Intercom access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "intercom");

            _logger.LogInformation("Intercom: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Intercom: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Intercom schema for '{resource}': {ex.Message}", ex, "intercom");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Intercom access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "intercom");

            _logger.LogInformation("Intercom: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Intercom: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Intercom dry run preview failed for '{resource}': {ex.Message}", ex, "intercom");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 0;

            // Resources that don't paginate — single GET.
            var nonPaginatedResources = new HashSet<string>
            {
                "admins", "teams", "tags", "segments", "data_attributes", "collections", "help_centers"
            };

            if (nonPaginatedResources.Contains(resource))
            {
                var endpoint = GetEndpoint(resource);
                using var request = new HttpRequestMessage(HttpMethod.Get, $"{BaseUrl}/{endpoint}");
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResponseData(doc.RootElement, resource, results);

                _logger.LogInformation("Intercom: read {Count} records from '{Resource}'.", results.Count, resource);
                return results;
            }

            // Paginated resources (contacts, companies, conversations, articles).
            var perPage = PageSizes.GetValueOrDefault(resource, 50);
            string? nextUrl = $"{BaseUrl}/{GetEndpoint(resource)}?per_page={perPage}";

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResponseData(doc.RootElement, resource, results);

                // Cursor-based pagination: "pages" → "next" URL.
                nextUrl = null;
                if (doc.RootElement.TryGetProperty("pages", out var pages))
                {
                    if (pages.TryGetProperty("next", out var next))
                    {
                        if (next.ValueKind == JsonValueKind.String)
                        {
                            nextUrl = next.GetString();
                        }
                        else if (next.ValueKind == JsonValueKind.Object
                            && next.TryGetProperty("starting_after", out var startingAfter))
                        {
                            // Some endpoints return { "next": { "starting_after": "..." } }.
                            var cursor = startingAfter.GetString();
                            if (!string.IsNullOrEmpty(cursor))
                            {
                                nextUrl = $"{BaseUrl}/{GetEndpoint(resource)}?per_page={perPage}&starting_after={Uri.EscapeDataString(cursor)}";
                            }
                        }
                    }
                }

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Intercom: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Endpoint mapping ─────────────────────────────────────────────────

        private static string GetEndpoint(string resource) => resource switch
        {
            "contacts"         => "contacts",
            "companies"        => "companies",
            "conversations"    => "conversations",
            "admins"           => "admins",
            "teams"            => "teams",
            "tags"             => "tags",
            "segments"         => "segments",
            "data_attributes"  => "data_attributes",
            "articles"         => "articles",
            "collections"      => "help_center/collections",
            "help_centers"     => "help_center/help_centers",
            _                  => throw new ConnectorException(
                $"Unsupported Intercom resource: '{resource}'.",
                new ArgumentException($"Unsupported resource: {resource}"),
                "intercom")
        };

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResponseData(JsonElement root, string resource, List<object> results)
        {
            JsonElement items;

            // Most Intercom responses wrap data in a "data" array.
            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard shape.
            }
            // Some responses use the resource name as the key (e.g. "admins", "tags").
            else if (root.TryGetProperty(resource, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Resource-keyed shape.
            }
            // Fallback: top-level array.
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
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten one level of nested objects.
                        foreach (var inner in prop.Value.EnumerateObject())
                        {
                            row[$"{prop.Name}_{inner.Name}"] = ConvertJsonValue(inner.Value);
                        }
                    }
                    else
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
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
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
                    $"Intercom connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "intercom");
            return value;
        }
    }
}
