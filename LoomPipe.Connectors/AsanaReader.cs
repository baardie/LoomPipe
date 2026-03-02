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
    /// Reads project and task data from Asana using the v1 API.
    ///
    /// Parameters:
    ///   accessToken — Asana personal access token (PAT) or OAuth token
    ///   resource    — tasks, projects, workspaces, users, teams, tags, portfolios,
    ///                 goals, sections, stories, custom_fields
    ///   workspace   — workspace GID (required for tasks, projects, users, teams, tags, portfolios, goals)
    ///   project     — project GID (required for tasks, sections, stories)
    /// </summary>
    public class AsanaReader : ISourceReader
    {
        private const string BaseUrl = "https://app.asana.com/api/1.0";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "tasks", "projects", "workspaces", "users", "teams", "tags",
            "portfolios", "goals", "sections", "stories", "custom_fields"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<AsanaReader> _logger;

        public AsanaReader(HttpClient httpClient, ILogger<AsanaReader> logger)
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
                    "Asana access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "asana");
            var workspace = GetStringParam(config.Parameters, "workspace");
            var project   = GetStringParam(config.Parameters, "project");

            _logger.LogInformation("Asana: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, workspace, project);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Asana: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Asana resource '{resource}': {ex.Message}", ex, "asana");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Asana access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "asana");
            var workspace = GetStringParam(config.Parameters, "workspace");
            var project   = GetStringParam(config.Parameters, "project");

            _logger.LogInformation("Asana: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, workspace, project, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Asana: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Asana schema for '{resource}': {ex.Message}", ex, "asana");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Asana access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "asana");
            var workspace = GetStringParam(config.Parameters, "workspace");
            var project   = GetStringParam(config.Parameters, "project");

            _logger.LogInformation("Asana: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, workspace, project, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Asana: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Asana dry run preview failed for '{resource}': {ex.Message}", ex, "asana");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken,
            string? workspace, string? project,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? offset = null;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, workspace, project, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonArray(data, results);
                }

                // Pagination: "next_page" → "offset".
                offset = null;
                if (doc.RootElement.TryGetProperty("next_page", out var nextPage)
                    && nextPage.ValueKind == JsonValueKind.Object
                    && nextPage.TryGetProperty("offset", out var offsetEl)
                    && offsetEl.ValueKind == JsonValueKind.String)
                {
                    offset = offsetEl.GetString();
                }

                page++;
            }
            while (offset != null && page < maxPages);

            _logger.LogInformation("Asana: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? workspace, string? project, string? offset)
        {
            var sb = new StringBuilder();

            switch (resource)
            {
                case "tasks":
                    if (string.IsNullOrEmpty(project))
                        throw new ConnectorException(
                            "Asana 'tasks' resource requires the 'project' parameter.",
                            new ArgumentException("Missing 'project' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/tasks?project={Uri.EscapeDataString(project)}&opt_fields=name,completed,due_on,assignee,created_at,modified_at,notes&limit={PageLimit}");
                    break;

                case "projects":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'projects' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/projects?workspace={Uri.EscapeDataString(workspace)}&opt_fields=name,owner,due_on,created_at,modified_at,public,archived&limit={PageLimit}");
                    break;

                case "workspaces":
                    sb.Append($"{BaseUrl}/workspaces?limit={PageLimit}");
                    break;

                case "users":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'users' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/users?workspace={Uri.EscapeDataString(workspace)}&opt_fields=name,email&limit={PageLimit}");
                    break;

                case "teams":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'teams' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/organizations/{Uri.EscapeDataString(workspace)}/teams?limit={PageLimit}");
                    break;

                case "tags":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'tags' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/tags?workspace={Uri.EscapeDataString(workspace)}&limit={PageLimit}");
                    break;

                case "portfolios":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'portfolios' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/portfolios?workspace={Uri.EscapeDataString(workspace)}&opt_fields=name,owner,created_at&limit={PageLimit}");
                    break;

                case "goals":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'goals' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/goals?workspace={Uri.EscapeDataString(workspace)}&limit={PageLimit}");
                    break;

                case "sections":
                    if (string.IsNullOrEmpty(project))
                        throw new ConnectorException(
                            "Asana 'sections' resource requires the 'project' parameter.",
                            new ArgumentException("Missing 'project' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/projects/{Uri.EscapeDataString(project)}/sections?limit={PageLimit}");
                    break;

                case "stories":
                    if (string.IsNullOrEmpty(project))
                        throw new ConnectorException(
                            "Asana 'stories' resource requires the 'project' parameter (used as task GID for stories).",
                            new ArgumentException("Missing 'project' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/tasks/{Uri.EscapeDataString(project)}/stories?limit={PageLimit}");
                    break;

                case "custom_fields":
                    if (string.IsNullOrEmpty(workspace))
                        throw new ConnectorException(
                            "Asana 'custom_fields' resource requires the 'workspace' parameter.",
                            new ArgumentException("Missing 'workspace' parameter."),
                            "asana");
                    sb.Append($"{BaseUrl}/workspaces/{Uri.EscapeDataString(workspace)}/custom_fields?limit={PageLimit}");
                    break;

                default:
                    throw new ConnectorException(
                        $"Unsupported Asana resource: '{resource}'.",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "asana");
            }

            if (!string.IsNullOrEmpty(offset))
            {
                sb.Append($"&offset={Uri.EscapeDataString(offset)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void FlattenJsonArray(JsonElement array, List<object> results)
        {
            foreach (var element in array.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten nested objects (e.g. assignee → assignee_gid, assignee_name).
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
                    $"Asana connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "asana");
            return value;
        }
    }
}
