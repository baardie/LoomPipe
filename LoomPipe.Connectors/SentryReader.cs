#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from Sentry using the REST API.
    ///
    /// Parameters:
    ///   accessToken      — Sentry auth token (Bearer)
    ///   resource         — issues, events, projects, organizations, teams, releases, tags, users
    ///   organizationSlug — Sentry organization slug
    ///   projectSlug      — Sentry project slug (required for project-scoped resources)
    /// </summary>
    public class SentryReader : ISourceReader
    {
        private const string BaseUrl = "https://sentry.io/api/0";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "issues", "events", "projects", "organizations", "teams",
            "releases", "tags", "users"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SentryReader> _logger;

        public SentryReader(HttpClient httpClient, ILogger<SentryReader> logger)
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
                    "Sentry access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sentry");

            _logger.LogInformation("Sentry: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(config, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sentry: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Sentry resource '{resource}': {ex.Message}", ex, "sentry");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Sentry access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sentry");

            _logger.LogInformation("Sentry: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sentry: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Sentry schema for '{resource}': {ex.Message}", ex, "sentry");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Sentry access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sentry");

            _logger.LogInformation("Sentry: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sentry: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Sentry dry run preview failed for '{resource}': {ex.Message}", ex, "sentry");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based pagination via Link header) ──────────────

        private async Task<List<object>> ReadFullAsync(
            DataSourceConfig config, string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var url = BuildUrl(config, resource);
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Sentry returns arrays directly for most endpoints
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }
                else if (root.ValueKind == JsonValueKind.Object)
                {
                    // Some endpoints may wrap results
                    results.Add(FlattenJsonObject(root));
                }

                // Pagination via Link header with cursor
                url = ParseNextLinkFromHeaders(response);
                page++;
            }
            while (url != null && page < maxPages);

            _logger.LogInformation("Sentry: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builders ─────────────────────────────────────────────────────

        private static string BuildUrl(DataSourceConfig config, string resource)
        {
            var orgSlug     = GetStringParam(config.Parameters, "organizationSlug");
            var projectSlug = GetStringParam(config.Parameters, "projectSlug");

            return resource switch
            {
                "issues" => BuildProjectScopedUrl(orgSlug, projectSlug, "issues", $"?cursor=&query=&limit={PageLimit}"),
                "events" => BuildProjectScopedUrl(orgSlug, projectSlug, "events", $"?limit={PageLimit}"),
                "projects" => !string.IsNullOrEmpty(orgSlug)
                    ? $"{BaseUrl}/organizations/{Uri.EscapeDataString(orgSlug)}/projects/?cursor=&limit={PageLimit}"
                    : $"{BaseUrl}/projects/?cursor=&limit={PageLimit}",
                "organizations" => $"{BaseUrl}/organizations/?cursor=&limit={PageLimit}",
                "teams" => !string.IsNullOrEmpty(orgSlug)
                    ? $"{BaseUrl}/organizations/{Uri.EscapeDataString(orgSlug)}/teams/?cursor=&limit={PageLimit}"
                    : throw new ConnectorException(
                        "Sentry 'teams' resource requires the 'organizationSlug' parameter.",
                        new ArgumentException("Missing required parameter: organizationSlug"),
                        "sentry"),
                "releases" => !string.IsNullOrEmpty(orgSlug)
                    ? $"{BaseUrl}/organizations/{Uri.EscapeDataString(orgSlug)}/releases/?cursor=&limit={PageLimit}"
                    : throw new ConnectorException(
                        "Sentry 'releases' resource requires the 'organizationSlug' parameter.",
                        new ArgumentException("Missing required parameter: organizationSlug"),
                        "sentry"),
                "tags" => BuildProjectScopedUrl(orgSlug, projectSlug, "tags", ""),
                "users" => !string.IsNullOrEmpty(orgSlug)
                    ? $"{BaseUrl}/organizations/{Uri.EscapeDataString(orgSlug)}/members/?cursor=&limit={PageLimit}"
                    : throw new ConnectorException(
                        "Sentry 'users' resource requires the 'organizationSlug' parameter.",
                        new ArgumentException("Missing required parameter: organizationSlug"),
                        "sentry"),
                _ => throw new ConnectorException(
                    $"Unknown Sentry resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "sentry")
            };
        }

        private static string BuildProjectScopedUrl(string? orgSlug, string? projectSlug, string endpoint, string queryString)
        {
            if (string.IsNullOrEmpty(orgSlug))
                throw new ConnectorException(
                    $"Sentry '{endpoint}' resource requires the 'organizationSlug' parameter.",
                    new ArgumentException("Missing required parameter: organizationSlug"),
                    "sentry");
            if (string.IsNullOrEmpty(projectSlug))
                throw new ConnectorException(
                    $"Sentry '{endpoint}' resource requires the 'projectSlug' parameter.",
                    new ArgumentException("Missing required parameter: projectSlug"),
                    "sentry");

            return $"{BaseUrl}/projects/{Uri.EscapeDataString(orgSlug)}/{Uri.EscapeDataString(projectSlug)}/{endpoint}/{queryString}";
        }

        // ── Link header pagination ──────────────────────────────────────────

        /// <summary>
        /// Parses the Link header to extract the next page URL.
        /// Sentry uses: Link: &lt;url&gt;; rel="previous"; results="false"; cursor="...",
        ///              &lt;url&gt;; rel="next"; results="true"; cursor="..."
        /// </summary>
        private static string? ParseNextLinkFromHeaders(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return null;

            foreach (var linkHeader in linkValues)
            {
                var parts = linkHeader.Split(',');
                foreach (var part in parts)
                {
                    var trimmed = part.Trim();
                    // Check for rel="next" and results="true"
                    if (trimmed.Contains("rel=\"next\"") && trimmed.Contains("results=\"true\""))
                    {
                        // Extract URL between < and >
                        var urlStart = trimmed.IndexOf('<');
                        var urlEnd = trimmed.IndexOf('>');
                        if (urlStart >= 0 && urlEnd > urlStart)
                        {
                            return trimmed.Substring(urlStart + 1, urlEnd - urlStart - 1);
                        }
                    }
                }
            }

            return null;
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

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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
                    $"Sentry connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "sentry");
            return value;
        }
    }
}
