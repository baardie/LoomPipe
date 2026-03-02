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
    /// Reads data from Confluence Cloud using the REST API v2.
    ///
    /// Parameters:
    ///   accessToken  — Confluence/Atlassian API token
    ///   email        — Atlassian account email (for Basic auth)
    ///   domain       — Confluence Cloud domain (e.g. "mycompany" → mycompany.atlassian.net)
    ///   resource     — pages, spaces, blog_posts, comments, attachments, labels,
    ///                  content_properties, tasks
    ///   spaceKey     — optional space key for filtering
    ///
    /// ConnectionString JSON: {"domain":"...","email":"...","accessToken":"..."}
    /// </summary>
    public class ConfluenceReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "pages", "spaces", "blog_posts", "comments", "attachments",
            "labels", "content_properties", "tasks"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ConfluenceReader> _logger;

        public ConfluenceReader(HttpClient httpClient, ILogger<ConfluenceReader> logger)
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
            var (domain, accessToken, email) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Confluence: reading resource '{Resource}' from '{Domain}'.", resource, domain);

            try
            {
                return await ReadFullAsync(config, domain, accessToken, email, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Confluence: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Confluence resource '{resource}': {ex.Message}", ex, "confluence");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var (domain, accessToken, email) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Confluence: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(config, domain, accessToken, email, resource, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Confluence: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Confluence schema for '{resource}': {ex.Message}", ex, "confluence");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (domain, accessToken, email) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Confluence: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(config, domain, accessToken, email, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Confluence: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Confluence dry run preview failed for '{resource}': {ex.Message}", ex, "confluence");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based pagination) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            DataSourceConfig config, string domain, string accessToken, string? email,
            string resource, int maxPages = int.MaxValue)
        {
            var baseUrl = $"https://{domain}.atlassian.net";
            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            do
            {
                var url = BuildUrl(baseUrl, config, resource, cursor);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse results array
                if (root.TryGetProperty("results", out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in items.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }

                // Cursor-based pagination: check _links.next
                cursor = null;
                if (root.TryGetProperty("_links", out var links)
                    && links.TryGetProperty("next", out var nextLink)
                    && nextLink.ValueKind == JsonValueKind.String)
                {
                    // The next link is a relative or absolute URL; extract cursor param.
                    var nextUrl = nextLink.GetString();
                    if (!string.IsNullOrEmpty(nextUrl))
                    {
                        cursor = nextUrl;
                    }
                }

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Confluence: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builders ─────────────────────────────────────────────────────

        private static string BuildUrl(string baseUrl, DataSourceConfig config, string resource, string? cursor)
        {
            // If we have a cursor (next link), it's typically a full relative path — use it directly.
            if (!string.IsNullOrEmpty(cursor))
            {
                // The cursor from _links.next is a relative path like /wiki/api/v2/pages?cursor=...
                return cursor.StartsWith("http", StringComparison.OrdinalIgnoreCase)
                    ? cursor
                    : $"{baseUrl}{cursor}";
            }

            var spaceKey = GetStringParam(config.Parameters, "spaceKey");

            return resource switch
            {
                "pages" => BuildPagesUrl(baseUrl, spaceKey),
                "spaces" => $"{baseUrl}/wiki/api/v2/spaces?limit={PageLimit}",
                "blog_posts" => BuildBlogPostsUrl(baseUrl, spaceKey),
                "comments" => $"{baseUrl}/wiki/api/v2/footer-comments?limit={PageLimit}",
                "attachments" => $"{baseUrl}/wiki/api/v2/attachments?limit={PageLimit}",
                "labels" => $"{baseUrl}/wiki/api/v2/labels?limit={PageLimit}",
                "content_properties" => $"{baseUrl}/wiki/api/v2/content-properties?limit={PageLimit}",
                "tasks" => $"{baseUrl}/wiki/api/v2/tasks?limit={PageLimit}",
                _ => throw new ConnectorException(
                    $"Unknown Confluence resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "confluence")
            };
        }

        private static string BuildPagesUrl(string baseUrl, string? spaceKey)
        {
            var sb = new StringBuilder($"{baseUrl}/wiki/api/v2/pages?limit={PageLimit}");
            if (!string.IsNullOrEmpty(spaceKey))
                sb.Append($"&space-key={Uri.EscapeDataString(spaceKey)}");
            return sb.ToString();
        }

        private static string BuildBlogPostsUrl(string baseUrl, string? spaceKey)
        {
            var sb = new StringBuilder($"{baseUrl}/wiki/api/v2/blogposts?limit={PageLimit}");
            if (!string.IsNullOrEmpty(spaceKey))
                sb.Append($"&space-key={Uri.EscapeDataString(spaceKey)}");
            return sb.ToString();
        }

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

        // ── Auth helpers ─────────────────────────────────────────────────────

        private (string domain, string accessToken, string? email) ResolveAuth(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var domain = GetRequiredParam(config.Parameters, "domain");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Confluence access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "confluence");
            var email = GetStringParam(config.Parameters, "email");

            return (domain, accessToken, email);
        }

        /// <summary>
        /// If ConnectionString is a JSON object with domain/email/accessToken, merge those
        /// into Parameters so the user can configure everything from the connection string.
        /// </summary>
        private static void MergeConnectionString(DataSourceConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString)) return;

            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) return;

                foreach (var prop in root.EnumerateObject())
                {
                    // Only merge if the parameter isn't already explicitly set.
                    if (!config.Parameters.ContainsKey(prop.Name) && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        config.Parameters[prop.Name] = prop.Value.GetString()!;
                    }
                }
            }
            catch (JsonException)
            {
                // Not JSON — treat as a plain access token.
            }
        }

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string? email)
        {
            if (!string.IsNullOrEmpty(email))
            {
                // Basic auth: base64(email:apiToken)
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{email}:{accessToken}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            }
            else
            {
                // Bearer token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            }
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
                    $"Confluence connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "confluence");
            return value;
        }
    }
}
