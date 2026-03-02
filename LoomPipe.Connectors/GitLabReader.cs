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
    /// Reads data from GitLab using the REST API v4.
    ///
    /// Parameters:
    ///   accessToken — GitLab personal access token or OAuth token
    ///   resource    — projects, issues, merge_requests, pipelines, jobs, commits, branches, tags,
    ///                 milestones, users, groups, releases, snippets, wiki_pages, environments
    ///   projectId   — required for project-scoped resources (issues, merge_requests, etc.)
    ///   baseUrl     — optional, defaults to https://gitlab.com (for self-hosted instances)
    /// </summary>
    public class GitLabReader : ISourceReader
    {
        private const string DefaultBaseUrl = "https://gitlab.com";
        private const int PerPage = 100;

        private static readonly string[] AllResources =
        {
            "projects", "issues", "merge_requests", "pipelines", "jobs",
            "commits", "branches", "tags", "milestones", "users",
            "groups", "releases", "snippets", "wiki_pages", "environments"
        };

        /// <summary>Resources that are scoped to a project and require projectId.</summary>
        private static readonly HashSet<string> ProjectScopedResources = new(StringComparer.OrdinalIgnoreCase)
        {
            "issues", "merge_requests", "pipelines", "jobs", "commits",
            "branches", "tags", "milestones", "releases", "snippets",
            "wiki_pages", "environments"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GitLabReader> _logger;

        public GitLabReader(HttpClient httpClient, ILogger<GitLabReader> logger)
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
                    "GitLab access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gitlab");
            var projectId   = GetStringParam(config.Parameters, "projectId");
            var baseUrl     = GetStringParam(config.Parameters, "baseUrl") ?? DefaultBaseUrl;

            _logger.LogInformation("GitLab: reading resource '{Resource}'.", resource);

            try
            {
                ValidateProjectScope(resource, projectId);

                var records = await ReadFullAsync(resource, accessToken, projectId, baseUrl);

                // Client-side watermark filtering if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records
                        .Where(r =>
                        {
                            var dict = r as IDictionary<string, object>;
                            if (dict == null || !dict.TryGetValue(watermarkField, out var val)) return false;
                            return string.Compare(val?.ToString(), watermarkValue, StringComparison.OrdinalIgnoreCase) > 0;
                        })
                        .ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitLab: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read GitLab resource '{resource}': {ex.Message}", ex, "gitlab");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "GitLab access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gitlab");
            var projectId   = GetStringParam(config.Parameters, "projectId");
            var baseUrl     = GetStringParam(config.Parameters, "baseUrl") ?? DefaultBaseUrl;

            _logger.LogInformation("GitLab: discovering schema for '{Resource}'.", resource);

            try
            {
                ValidateProjectScope(resource, projectId);

                var sample = await ReadFullAsync(resource, accessToken, projectId, baseUrl, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitLab: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover GitLab schema for '{resource}': {ex.Message}", ex, "gitlab");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "GitLab access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gitlab");
            var projectId   = GetStringParam(config.Parameters, "projectId");
            var baseUrl     = GetStringParam(config.Parameters, "baseUrl") ?? DefaultBaseUrl;

            _logger.LogInformation("GitLab: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                ValidateProjectScope(resource, projectId);

                var records = await ReadFullAsync(resource, accessToken, projectId, baseUrl, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitLab: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"GitLab dry run preview failed for '{resource}': {ex.Message}", ex, "gitlab");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? projectId, string baseUrl,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;
            int totalPages = int.MaxValue;

            do
            {
                var url = BuildListUrl(resource, projectId, baseUrl, page);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                // Read X-Total-Pages header for pagination bounds.
                if (response.Headers.TryGetValues("X-Total-Pages", out var totalPagesValues))
                {
                    var tpStr = totalPagesValues.FirstOrDefault();
                    if (int.TryParse(tpStr, out var tp))
                        totalPages = tp;
                }

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);

                int added = results.Count - countBefore;
                if (added == 0) break;

                page++;
            }
            while (page <= totalPages && page <= maxPages);

            _logger.LogInformation("GitLab: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page - 1);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? projectId, string baseUrl, int page)
        {
            var apiBase = $"{baseUrl.TrimEnd('/')}/api/v4";

            if (ProjectScopedResources.Contains(resource))
            {
                var encodedProjectId = Uri.EscapeDataString(projectId!);
                var endpoint = resource switch
                {
                    "wiki_pages" => $"{apiBase}/projects/{encodedProjectId}/wikis",
                    _            => $"{apiBase}/projects/{encodedProjectId}/{resource}"
                };
                return $"{endpoint}?per_page={PerPage}&page={page}";
            }

            return $"{apiBase}/{resource}?per_page={PerPage}&page={page}";
        }

        // ── Validation ───────────────────────────────────────────────────────

        private static void ValidateProjectScope(string resource, string? projectId)
        {
            if (ProjectScopedResources.Contains(resource) && string.IsNullOrWhiteSpace(projectId))
            {
                throw new ConnectorException(
                    $"GitLab resource '{resource}' requires the 'projectId' parameter.",
                    new ArgumentException($"Missing 'projectId' for project-scoped resource: {resource}"),
                    "gitlab");
            }
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Some endpoints wrap in "results"
            }
            else
            {
                // Single object response
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(root, row);
                results.Add(row);
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(element, row);
                results.Add(row);
            }
        }

        private static void FlattenJsonObject(JsonElement element, IDictionary<string, object> row)
        {
            if (element.ValueKind != JsonValueKind.Object) return;

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
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
            // GitLab supports both PRIVATE-TOKEN header and Bearer token.
            // Use PRIVATE-TOKEN for personal access tokens (more common).
            request.Headers.Add("PRIVATE-TOKEN", accessToken);
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
                    $"GitLab connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "gitlab");
            return value;
        }
    }
}
