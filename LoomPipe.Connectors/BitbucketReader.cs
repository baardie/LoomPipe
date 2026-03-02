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
    /// Reads data from Bitbucket Cloud using the REST API 2.0.
    ///
    /// Parameters:
    ///   accessToken — Bitbucket app password (used with Basic auth username:app_password) or OAuth bearer token
    ///   resource    — repositories, pull_requests, commits, branches, issues, pipelines,
    ///                 environments, deployments, downloads, watchers
    ///   workspace   — Bitbucket workspace slug (required)
    ///   repoSlug    — repository slug (required for repo-scoped resources)
    /// </summary>
    public class BitbucketReader : ISourceReader
    {
        private const string BaseUrl = "https://api.bitbucket.org/2.0";
        private const int DefaultPageLen = 100;

        private static readonly string[] AllResources =
        {
            "repositories", "pull_requests", "commits", "branches", "issues",
            "pipelines", "environments", "deployments", "downloads", "watchers"
        };

        /// <summary>Resources that are scoped to a repository and require repoSlug.</summary>
        private static readonly HashSet<string> RepoScopedResources = new(StringComparer.OrdinalIgnoreCase)
        {
            "pull_requests", "commits", "branches", "issues", "pipelines",
            "environments", "deployments", "downloads", "watchers"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<BitbucketReader> _logger;

        public BitbucketReader(HttpClient httpClient, ILogger<BitbucketReader> logger)
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
                    "Bitbucket access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bitbucket");
            var workspace   = GetRequiredParam(config.Parameters, "workspace");
            var repoSlug    = GetStringParam(config.Parameters, "repoSlug");

            _logger.LogInformation("Bitbucket: reading resource '{Resource}'.", resource);

            try
            {
                ValidateRepoScope(resource, repoSlug);

                var records = await ReadFullAsync(resource, accessToken, workspace, repoSlug);

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
                _logger.LogError(ex, "Bitbucket: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Bitbucket resource '{resource}': {ex.Message}", ex, "bitbucket");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Bitbucket access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bitbucket");
            var workspace   = GetRequiredParam(config.Parameters, "workspace");
            var repoSlug    = GetStringParam(config.Parameters, "repoSlug");

            _logger.LogInformation("Bitbucket: discovering schema for '{Resource}'.", resource);

            try
            {
                ValidateRepoScope(resource, repoSlug);

                var sample = await ReadFullAsync(resource, accessToken, workspace, repoSlug, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Bitbucket: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Bitbucket schema for '{resource}': {ex.Message}", ex, "bitbucket");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Bitbucket access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bitbucket");
            var workspace   = GetRequiredParam(config.Parameters, "workspace");
            var repoSlug    = GetStringParam(config.Parameters, "repoSlug");

            _logger.LogInformation("Bitbucket: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                ValidateRepoScope(resource, repoSlug);

                var records = await ReadFullAsync(resource, accessToken, workspace, repoSlug, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Bitbucket: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Bitbucket dry run preview failed for '{resource}': {ex.Message}", ex, "bitbucket");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET with "next" URL) ────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string workspace, string? repoSlug,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextUrl = BuildListUrl(resource, workspace, repoSlug);
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);

                int added = results.Count - countBefore;
                if (added == 0) break;

                // Bitbucket pagination uses a "next" URL in the response body.
                nextUrl = null;
                if (doc.RootElement.TryGetProperty("next", out var nextProp)
                    && nextProp.ValueKind == JsonValueKind.String)
                {
                    nextUrl = nextProp.GetString();
                }

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Bitbucket: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page + 1);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string workspace, string? repoSlug)
        {
            var escapedWorkspace = Uri.EscapeDataString(workspace);

            if (!RepoScopedResources.Contains(resource))
            {
                // Workspace-level resources
                return resource switch
                {
                    "repositories" => $"{BaseUrl}/repositories/{escapedWorkspace}?pagelen={DefaultPageLen}",
                    _ => throw new ConnectorException(
                        $"Unknown Bitbucket resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "bitbucket")
                };
            }

            var escapedRepo = Uri.EscapeDataString(repoSlug!);
            var pagelen = resource == "pull_requests" ? 50 : DefaultPageLen;

            var endpoint = resource switch
            {
                "pull_requests" => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/pullrequests",
                "commits"       => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/commits",
                "branches"      => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/refs/branches",
                "issues"        => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/issues",
                "pipelines"     => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/pipelines",
                "environments"  => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/environments",
                "deployments"   => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/deployments",
                "downloads"     => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/downloads",
                "watchers"      => $"{BaseUrl}/repositories/{escapedWorkspace}/{escapedRepo}/watchers",
                _ => throw new ConnectorException(
                    $"Unknown Bitbucket resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "bitbucket")
            };

            return $"{endpoint}?pagelen={pagelen}";
        }

        // ── Validation ───────────────────────────────────────────────────────

        private static void ValidateRepoScope(string resource, string? repoSlug)
        {
            if (RepoScopedResources.Contains(resource) && string.IsNullOrWhiteSpace(repoSlug))
            {
                throw new ConnectorException(
                    $"Bitbucket resource '{resource}' requires the 'repoSlug' parameter.",
                    new ArgumentException($"Missing 'repoSlug' for repo-scoped resource: {resource}"),
                    "bitbucket");
            }
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            // Bitbucket wraps paginated results in a "values" array.
            if (root.TryGetProperty("values", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard paginated response
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
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
            // Support both Bearer token (OAuth) and Basic auth.
            // If the token contains a colon, treat it as username:app_password for Basic auth.
            if (accessToken.Contains(':'))
            {
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes(accessToken));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            }
            else
            {
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
                    $"Bitbucket connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "bitbucket");
            return value;
        }
    }
}
