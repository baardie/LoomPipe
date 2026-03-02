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
    /// Reads data from Linear using the GraphQL API.
    ///
    /// Parameters:
    ///   accessToken — Linear API key or OAuth token
    ///   resource    — issues, projects, teams, users, cycles, labels, milestones,
    ///                 comments, workflows, integrations
    /// </summary>
    public class LinearReader : ISourceReader
    {
        private const string ApiUrl = "https://api.linear.app/graphql";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "issues", "projects", "teams", "users", "cycles", "labels",
            "milestones", "comments", "workflows", "integrations"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<LinearReader> _logger;

        public LinearReader(HttpClient httpClient, ILogger<LinearReader> logger)
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
                    "Linear access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "linear");

            _logger.LogInformation("Linear: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Linear: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Linear resource '{resource}': {ex.Message}", ex, "linear");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Linear access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "linear");

            _logger.LogInformation("Linear: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Linear: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Linear schema for '{resource}': {ex.Message}", ex, "linear");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Linear access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "linear");

            _logger.LogInformation("Linear: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Linear: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Linear dry run preview failed for '{resource}': {ex.Message}", ex, "linear");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GraphQL) ────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            do
            {
                var query = BuildQuery(resource, cursor);
                var root = await ExecuteGraphQLAsync(accessToken, query);

                if (!root.TryGetProperty("data", out var data))
                    break;

                // Resolve the connection node name.
                var connectionName = GetConnectionName(resource);
                if (!data.TryGetProperty(connectionName, out var connection))
                    break;

                // Parse nodes.
                if (connection.TryGetProperty("nodes", out var nodes) && nodes.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in nodes.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        foreach (var prop in element.EnumerateObject())
                        {
                            if (prop.Value.ValueKind == JsonValueKind.Object)
                            {
                                // Flatten nested objects (e.g. state → state_name, assignee → assignee_name).
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

                // Pagination: pageInfo → hasNextPage + endCursor.
                cursor = null;
                if (connection.TryGetProperty("pageInfo", out var pageInfo))
                {
                    var hasNextPage = pageInfo.TryGetProperty("hasNextPage", out var hnp) && hnp.GetBoolean();
                    if (hasNextPage && pageInfo.TryGetProperty("endCursor", out var ec) && ec.ValueKind == JsonValueKind.String)
                    {
                        cursor = ec.GetString();
                    }
                }

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Linear: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── GraphQL query builder ────────────────────────────────────────────

        private static string BuildQuery(string resource, string? cursor)
        {
            var afterClause = cursor != null ? $", after: \"{cursor.Replace("\"", "\\\"")}\"" : "";
            var connectionName = GetConnectionName(resource);

            var fieldsFragment = resource switch
            {
                "issues" =>
                    "nodes { id title description priority number createdAt updatedAt state { name } assignee { name } team { name } }",
                "projects" =>
                    "nodes { id name description state createdAt updatedAt startDate targetDate }",
                "teams" =>
                    "nodes { id name key description createdAt }",
                "users" =>
                    "nodes { id name displayName email active admin createdAt }",
                "cycles" =>
                    "nodes { id number name startsAt endsAt createdAt completedAt }",
                "labels" =>
                    "nodes { id name color createdAt }",
                "milestones" =>
                    "nodes { id name description sortOrder createdAt updatedAt }",
                "comments" =>
                    "nodes { id body createdAt updatedAt user { name } issue { id title } }",
                "workflows" =>
                    "nodes { id name type position color }",
                "integrations" =>
                    "nodes { id service createdAt }",
                _ => throw new ConnectorException(
                    $"Unsupported Linear resource: '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "linear")
            };

            return $"{{ {connectionName}(first: {PageLimit}{afterClause}) {{ {fieldsFragment} pageInfo {{ hasNextPage endCursor }} }} }}";
        }

        /// <summary>
        /// Maps resource names to their Linear GraphQL connection field names.
        /// Most are identical, but some differ (e.g. milestones → milestones, workflows → workflowStates).
        /// </summary>
        private static string GetConnectionName(string resource) => resource switch
        {
            "workflows"    => "workflowStates",
            "milestones"   => "projectMilestones",
            _              => resource
        };

        // ── GraphQL execution ────────────────────────────────────────────────

        private async Task<JsonElement> ExecuteGraphQLAsync(string accessToken, string query)
        {
            var bodyObj = new { query };
            var bodyJson = JsonSerializer.Serialize(bodyObj);

            using var request = new HttpRequestMessage(HttpMethod.Post, ApiUrl)
            {
                Content = new StringContent(bodyJson, Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            // Check for GraphQL errors.
            if (doc.RootElement.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array)
            {
                var firstError = errors.EnumerateArray().FirstOrDefault();
                var message = firstError.TryGetProperty("message", out var msg) ? msg.GetString() : "Unknown GraphQL error";
                throw new ConnectorException(
                    $"Linear GraphQL error: {message}",
                    new InvalidOperationException(message ?? "Unknown GraphQL error"),
                    "linear");
            }

            return doc.RootElement.Clone();
        }

        // ── Response parsing ─────────────────────────────────────────────────

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
                    $"Linear connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "linear");
            return value;
        }
    }
}
