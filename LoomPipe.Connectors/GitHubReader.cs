#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from the GitHub REST API v3.
    ///
    /// Parameters:
    ///   accessToken  — GitHub Personal Access Token (PAT)
    ///   resource     — repos, issues, pulls, commits, releases, branches, tags,
    ///                  milestones, labels, contributors, workflows, actions,
    ///                  stargazers, forks, comments
    ///   owner        — repository owner (user or org)
    ///   repo         — repository name
    ///   state        — for issues/pulls: open, closed, all (default: open)
    /// </summary>
    public class GitHubReader : ISourceReader
    {
        private const string BaseUrl = "https://api.github.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "repos", "issues", "pulls", "commits", "releases",
            "branches", "tags", "milestones", "labels", "contributors",
            "workflows", "actions", "stargazers", "forks", "comments"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GitHubReader> _logger;

        public GitHubReader(HttpClient httpClient, ILogger<GitHubReader> logger)
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
                    "GitHub access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "github");

            _logger.LogInformation("GitHub: reading resource '{Resource}'.", resource);

            try
            {
                var url = BuildResourceUrl(config.Parameters, resource);
                var records = await ReadPaginatedAsync(url, accessToken);

                // Apply watermark filter client-side if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records.Where(r =>
                    {
                        if (r is IDictionary<string, object> dict && dict.TryGetValue(watermarkField, out var val))
                        {
                            return string.Compare(val?.ToString() ?? "", watermarkValue, StringComparison.Ordinal) > 0;
                        }
                        return false;
                    }).ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitHub: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read GitHub resource '{resource}': {ex.Message}", ex, "github");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "GitHub access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "github");

            _logger.LogInformation("GitHub: discovering schema for '{Resource}'.", resource);

            try
            {
                var url = BuildResourceUrl(config.Parameters, resource);
                var records = await ReadPaginatedAsync(url, accessToken, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitHub: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover GitHub schema for '{resource}': {ex.Message}", ex, "github");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "GitHub access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "github");

            _logger.LogInformation("GitHub: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var url = BuildResourceUrl(config.Parameters, resource);
                var records = await ReadPaginatedAsync(url, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GitHub: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"GitHub dry run preview failed for '{resource}': {ex.Message}", ex, "github");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildResourceUrl(Dictionary<string, object> parameters, string resource)
        {
            var owner = GetStringParam(parameters, "owner");
            var repo  = GetStringParam(parameters, "repo");
            var state = GetStringParam(parameters, "state") ?? "open";

            return resource switch
            {
                "repos" => !string.IsNullOrEmpty(owner)
                    ? $"{BaseUrl}/orgs/{Uri.EscapeDataString(owner)}/repos?per_page={PageLimit}"
                    : $"{BaseUrl}/user/repos?per_page={PageLimit}",

                "issues" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/issues?state={Uri.EscapeDataString(state)}&per_page={PageLimit}",

                "pulls" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/pulls?state={Uri.EscapeDataString(state)}&per_page={PageLimit}",

                "commits" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/commits?per_page={PageLimit}",

                "releases" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/releases?per_page={PageLimit}",

                "branches" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/branches?per_page={PageLimit}",

                "tags" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/tags?per_page={PageLimit}",

                "milestones" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/milestones?per_page={PageLimit}",

                "labels" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/labels?per_page={PageLimit}",

                "contributors" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/contributors?per_page={PageLimit}",

                "workflows" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/actions/workflows?per_page={PageLimit}",

                "actions" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/actions/runs?per_page={PageLimit}",

                "stargazers" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/stargazers?per_page={PageLimit}",

                "forks" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/forks?per_page={PageLimit}",

                "comments" => $"{BaseUrl}/repos/{Uri.EscapeDataString(owner!)}/{Uri.EscapeDataString(repo!)}/issues/comments?per_page={PageLimit}",

                _ => throw new ConnectorException(
                    $"Unknown GitHub resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "github")
            };
        }

        // ── Paginated read (Link header) ─────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string url, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextUrl = url;
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Some endpoints return an object with an array property (workflows, actions).
                JsonElement items;
                if (root.ValueKind == JsonValueKind.Array)
                {
                    items = root;
                }
                else if (root.TryGetProperty("workflows", out var workflows) && workflows.ValueKind == JsonValueKind.Array)
                {
                    items = workflows;
                }
                else if (root.TryGetProperty("workflow_runs", out var runs) && runs.ValueKind == JsonValueKind.Array)
                {
                    items = runs;
                }
                else
                {
                    // Single object response — wrap it.
                    results.Add(FlattenJsonObject(root));
                    break;
                }

                foreach (var element in items.EnumerateArray())
                {
                    results.Add(FlattenJsonObject(element));
                }

                // Parse Link header for next page.
                nextUrl = ParseNextLink(response);
                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("GitHub: read {Count} records across {Pages} page(s).", results.Count, page);
            return results;
        }

        /// <summary>
        /// Parses the GitHub Link header to extract the "next" URL.
        /// Format: &lt;url&gt;; rel="next", &lt;url&gt;; rel="last"
        /// </summary>
        private static string? ParseNextLink(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return null;

            var linkHeader = string.Join(",", linkValues);
            var match = Regex.Match(linkHeader, @"<([^>]+)>;\s*rel=""next""");
            return match.Success ? match.Groups[1].Value : null;
        }

        // ── JSON flattening ──────────────────────────────────────────────────

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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
            request.Headers.TryAddWithoutValidation("X-GitHub-Api-Version", "2022-11-28");
            if (!request.Headers.Contains("User-Agent"))
                request.Headers.TryAddWithoutValidation("User-Agent", "LoomPipe-Connector");
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
                    $"GitHub connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "github");
            return value;
        }
    }
}
