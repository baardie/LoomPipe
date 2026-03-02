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
    /// Reads data from Jira Cloud using the REST API v3.
    ///
    /// Parameters:
    ///   accessToken  — Jira API token (used with email for Basic auth, or as Bearer token)
    ///   email        — Jira account email (for Basic auth)
    ///   domain       — Jira Cloud domain (e.g. "mycompany" → mycompany.atlassian.net)
    ///   resource     — issues, projects, users, boards, sprints, worklogs, components,
    ///                  versions, fields, priorities, statuses
    ///   project      — project key for filtering issues (optional)
    ///   jql          — JIRA Query Language expression (optional, overrides project filter)
    /// </summary>
    public class JiraReader : ISourceReader
    {
        private const int PageLimit = 50;

        private static readonly string[] AllResources =
        {
            "issues", "projects", "users", "boards", "sprints",
            "worklogs", "components", "versions", "fields",
            "priorities", "statuses"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<JiraReader> _logger;

        public JiraReader(HttpClient httpClient, ILogger<JiraReader> logger)
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

            _logger.LogInformation("Jira: reading resource '{Resource}' from '{Domain}'.", resource, domain);

            try
            {
                // If a watermark is provided for issues, append it to the JQL.
                if (resource == "issues" && !string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    var jql = GetStringParam(config.Parameters, "jql") ?? "";
                    var wmClause = $"{watermarkField} > \"{watermarkValue}\"";
                    jql = string.IsNullOrEmpty(jql) ? wmClause : $"({jql}) AND {wmClause}";
                    return await ReadIssuesAsync(domain, accessToken, email, jql);
                }

                return await ReadResourceAsync(config, domain, accessToken, email, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Jira: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Jira resource '{resource}': {ex.Message}", ex, "jira");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var (domain, accessToken, email) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Jira: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadResourceAsync(config, domain, accessToken, email, resource, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Jira: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Jira schema for '{resource}': {ex.Message}", ex, "jira");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (domain, accessToken, email) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Jira: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadResourceAsync(config, domain, accessToken, email, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Jira: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Jira dry run preview failed for '{resource}': {ex.Message}", ex, "jira");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Resource dispatch ─────────────────────────────────────────────────

        private async Task<List<object>> ReadResourceAsync(
            DataSourceConfig config, string domain, string accessToken, string? email,
            string resource, int maxPages = int.MaxValue)
        {
            return resource switch
            {
                "issues"     => await ReadIssuesAsync(domain, accessToken, email,
                                    GetStringParam(config.Parameters, "jql")
                                    ?? (GetStringParam(config.Parameters, "project") is { } proj
                                        ? $"project = {proj}" : ""),
                                    maxPages),
                "projects"   => await ReadSimpleAsync(domain, accessToken, email, "/rest/api/3/project"),
                "users"      => await ReadSimpleAsync(domain, accessToken, email, "/rest/api/3/users/search?maxResults=1000"),
                "boards"     => await ReadAgilePagedAsync(domain, accessToken, email, "/rest/agile/1.0/board", "values", maxPages),
                "sprints"    => await ReadSprintsAsync(config, domain, accessToken, email, maxPages),
                "worklogs"   => await ReadWorklogsAsync(config, domain, accessToken, email),
                "components" => await ReadProjectSubresourceAsync(config, domain, accessToken, email, "components"),
                "versions"   => await ReadProjectSubresourceAsync(config, domain, accessToken, email, "versions"),
                "fields"     => await ReadSimpleAsync(domain, accessToken, email, "/rest/api/3/field"),
                "priorities" => await ReadSimpleAsync(domain, accessToken, email, "/rest/api/3/priority"),
                "statuses"   => await ReadSimpleAsync(domain, accessToken, email, "/rest/api/3/status"),
                _            => throw new ConnectorException(
                                    $"Unknown Jira resource '{resource}'.",
                                    new ArgumentException($"Unsupported resource: {resource}"),
                                    "jira")
            };
        }

        // ── Issues (JQL search with pagination) ──────────────────────────────

        private async Task<List<object>> ReadIssuesAsync(
            string domain, string accessToken, string? email, string jql, int maxPages = int.MaxValue)
        {
            var baseUrl = $"https://{domain}.atlassian.net";
            var results = new List<object>();
            int startAt = 0;
            int page = 0;

            do
            {
                var url = $"{baseUrl}/rest/api/3/search?jql={Uri.EscapeDataString(jql)}&startAt={startAt}&maxResults={PageLimit}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var total = root.TryGetProperty("total", out var totalEl) ? totalEl.GetInt32() : 0;

                if (root.TryGetProperty("issues", out var issues) && issues.ValueKind == JsonValueKind.Array)
                {
                    foreach (var issue in issues.EnumerateArray())
                    {
                        results.Add(FlattenIssue(issue));
                    }
                }

                startAt += PageLimit;
                page++;

                if (startAt >= total) break;
            }
            while (page < maxPages);

            _logger.LogInformation("Jira: read {Count} issues across {Pages} page(s).", results.Count, page);
            return results;
        }

        private static object FlattenIssue(JsonElement issue)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (issue.TryGetProperty("id", out var id))
                row["id"] = id.GetString() ?? id.ToString();
            if (issue.TryGetProperty("key", out var key))
                row["key"] = key.GetString() ?? key.ToString();
            if (issue.TryGetProperty("self", out var self))
                row["self"] = self.GetString() ?? self.ToString();

            // Flatten top-level fields from the "fields" sub-object.
            if (issue.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in fields.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }
            }

            return row;
        }

        // ── Simple array endpoint (no pagination) ────────────────────────────

        private async Task<List<object>> ReadSimpleAsync(
            string domain, string accessToken, string? email, string path)
        {
            var url = $"https://{domain}.atlassian.net{path}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken, email);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();
            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in root.EnumerateArray())
                    results.Add(FlattenJsonObject(el));
            }
            else if (root.TryGetProperty("values", out var values) && values.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in values.EnumerateArray())
                    results.Add(FlattenJsonObject(el));
            }

            _logger.LogInformation("Jira: read {Count} records from '{Path}'.", results.Count, path);
            return results;
        }

        // ── Agile paginated endpoint (boards) ────────────────────────────────

        private async Task<List<object>> ReadAgilePagedAsync(
            string domain, string accessToken, string? email,
            string path, string arrayProperty, int maxPages = int.MaxValue)
        {
            var baseUrl = $"https://{domain}.atlassian.net";
            var results = new List<object>();
            int startAt = 0;
            int page = 0;
            bool isLast = false;

            do
            {
                var url = $"{baseUrl}{path}?startAt={startAt}&maxResults={PageLimit}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, email);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty(arrayProperty, out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in items.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }

                isLast = root.TryGetProperty("isLast", out var isLastEl) && isLastEl.GetBoolean();
                startAt += PageLimit;
                page++;
            }
            while (!isLast && page < maxPages);

            _logger.LogInformation("Jira: read {Count} records from '{Path}' across {Pages} page(s).", results.Count, path, page);
            return results;
        }

        // ── Sprints (requires board context) ─────────────────────────────────

        private async Task<List<object>> ReadSprintsAsync(
            DataSourceConfig config, string domain, string accessToken, string? email, int maxPages)
        {
            // First, find all boards, then read sprints from each board.
            var boards = await ReadAgilePagedAsync(domain, accessToken, email, "/rest/agile/1.0/board", "values", maxPages);
            var results = new List<object>();

            foreach (var board in boards)
            {
                if (board is IDictionary<string, object> boardDict && boardDict.TryGetValue("id", out var boardId))
                {
                    var sprints = await ReadAgilePagedAsync(
                        domain, accessToken, email,
                        $"/rest/agile/1.0/board/{boardId}/sprint", "values", maxPages);
                    results.AddRange(sprints);
                }
            }

            _logger.LogInformation("Jira: read {Count} sprints from {BoardCount} board(s).", results.Count, boards.Count);
            return results;
        }

        // ── Worklogs (requires issue context or updated worklogs endpoint) ───

        private async Task<List<object>> ReadWorklogsAsync(
            DataSourceConfig config, string domain, string accessToken, string? email)
        {
            // Use the updated worklogs endpoint which returns IDs of worklogs updated since a given time.
            var baseUrl = $"https://{domain}.atlassian.net";
            var url = $"{baseUrl}/rest/api/3/worklog/updated?since=0";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken, email);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var results = new List<object>();
            if (root.TryGetProperty("values", out var values) && values.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in values.EnumerateArray())
                    results.Add(FlattenJsonObject(el));
            }

            _logger.LogInformation("Jira: read {Count} worklog entries.", results.Count);
            return results;
        }

        // ── Project sub-resources (components, versions) ─────────────────────

        private async Task<List<object>> ReadProjectSubresourceAsync(
            DataSourceConfig config, string domain, string accessToken, string? email, string subresource)
        {
            var project = GetStringParam(config.Parameters, "project")
                ?? throw new ConnectorException(
                    $"Jira '{subresource}' resource requires the 'project' parameter.",
                    new ArgumentException($"Missing required parameter: project"),
                    "jira");

            return await ReadSimpleAsync(domain, accessToken, email, $"/rest/api/3/project/{Uri.EscapeDataString(project)}/{subresource}");
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

        // ── Auth helpers ─────────────────────────────────────────────────────

        private (string domain, string accessToken, string? email) ResolveAuth(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var domain = GetRequiredParam(config.Parameters, "domain");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Jira access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "jira");
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
                    $"Jira connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "jira");
            return value;
        }
    }
}
