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
    /// Reads data from Monday.com using the GraphQL API v2.
    ///
    /// Parameters:
    ///   accessToken — Monday.com API key (v2 token)
    ///   resource    — boards, items, groups, columns, users, workspaces, tags
    ///   boardId     — board ID (required for items, groups, columns)
    /// </summary>
    public class MondayReader : ISourceReader
    {
        private const string ApiUrl = "https://api.monday.com/v2";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "boards", "items", "groups", "columns", "users", "workspaces", "tags"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MondayReader> _logger;

        public MondayReader(HttpClient httpClient, ILogger<MondayReader> logger)
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
                    "Monday.com API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "monday");
            var boardId = GetStringParam(config.Parameters, "boardId");

            _logger.LogInformation("Monday: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, boardId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monday: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Monday.com resource '{resource}': {ex.Message}", ex, "monday");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Monday.com API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "monday");
            var boardId = GetStringParam(config.Parameters, "boardId");

            _logger.LogInformation("Monday: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, boardId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monday: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Monday.com schema for '{resource}': {ex.Message}", ex, "monday");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Monday.com API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "monday");
            var boardId = GetStringParam(config.Parameters, "boardId");

            _logger.LogInformation("Monday: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, boardId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monday: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Monday.com dry run preview failed for '{resource}': {ex.Message}", ex, "monday");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GraphQL) ────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? boardId, int maxPages = int.MaxValue)
        {
            return resource switch
            {
                "boards"     => await ReadBoardsAsync(accessToken, maxPages),
                "items"      => await ReadItemsAsync(accessToken, boardId, maxPages),
                "groups"     => await ReadGroupsAsync(accessToken, boardId),
                "columns"    => await ReadColumnsAsync(accessToken, boardId),
                "users"      => await ReadUsersAsync(accessToken, maxPages),
                "workspaces" => await ReadWorkspacesAsync(accessToken, maxPages),
                "tags"       => await ReadTagsAsync(accessToken),
                _            => throw new ConnectorException(
                    $"Unsupported Monday.com resource: '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "monday")
            };
        }

        // ── Boards ───────────────────────────────────────────────────────────

        private async Task<List<object>> ReadBoardsAsync(string accessToken, int maxPages)
        {
            var results = new List<object>();
            int page = 1;

            do
            {
                var query = $"{{ boards(limit:{PageLimit}, page:{page}) {{ id name state board_kind description created_at }} }}";
                var root = await ExecuteGraphQLAsync(accessToken, query);

                if (!root.TryGetProperty("data", out var data)
                    || !data.TryGetProperty("boards", out var boards)
                    || boards.ValueKind != JsonValueKind.Array)
                    break;

                int count = 0;
                foreach (var element in boards.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    foreach (var prop in element.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                    results.Add(row);
                    count++;
                }

                if (count < PageLimit) break;
                page++;
            }
            while (page <= maxPages);

            _logger.LogInformation("Monday: read {Count} boards across {Pages} page(s).", results.Count, page);
            return results;
        }

        // ── Items (cursor-based pagination) ──────────────────────────────────

        private async Task<List<object>> ReadItemsAsync(string accessToken, string? boardId, int maxPages)
        {
            if (string.IsNullOrEmpty(boardId))
                throw new ConnectorException(
                    "Monday.com 'items' resource requires the 'boardId' parameter.",
                    new ArgumentException("Missing 'boardId' parameter."),
                    "monday");

            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            do
            {
                string query;
                if (cursor == null)
                {
                    query = $"{{ boards(ids:[{boardId}]) {{ items_page(limit:{PageLimit}) {{ cursor items {{ id name group {{ id title }} column_values {{ id title text }} }} }} }} }}";
                }
                else
                {
                    var escapedCursor = cursor.Replace("\"", "\\\"");
                    query = $"{{ next_items_page(limit:{PageLimit}, cursor:\"{escapedCursor}\") {{ cursor items {{ id name group {{ id title }} column_values {{ id title text }} }} }} }}";
                }

                var root = await ExecuteGraphQLAsync(accessToken, query);

                JsonElement itemsPage;
                if (cursor == null)
                {
                    if (!root.TryGetProperty("data", out var data)
                        || !data.TryGetProperty("boards", out var boards)
                        || boards.ValueKind != JsonValueKind.Array)
                        break;

                    var boardEl = boards.EnumerateArray().FirstOrDefault();
                    if (boardEl.ValueKind == JsonValueKind.Undefined
                        || !boardEl.TryGetProperty("items_page", out itemsPage))
                        break;
                }
                else
                {
                    if (!root.TryGetProperty("data", out var data)
                        || !data.TryGetProperty("next_items_page", out itemsPage))
                        break;
                }

                // Parse items.
                cursor = null;
                if (itemsPage.TryGetProperty("cursor", out var cursorEl)
                    && cursorEl.ValueKind == JsonValueKind.String)
                {
                    cursor = cursorEl.GetString();
                }

                if (itemsPage.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        if (element.TryGetProperty("id", out var id))
                            row["id"] = ConvertJsonValue(id);
                        if (element.TryGetProperty("name", out var name))
                            row["name"] = ConvertJsonValue(name);

                        // Flatten group.
                        if (element.TryGetProperty("group", out var group) && group.ValueKind == JsonValueKind.Object)
                        {
                            if (group.TryGetProperty("id", out var gid))
                                row["group_id"] = ConvertJsonValue(gid);
                            if (group.TryGetProperty("title", out var gtitle))
                                row["group_title"] = ConvertJsonValue(gtitle);
                        }

                        // Flatten column_values into individual columns.
                        if (element.TryGetProperty("column_values", out var cols) && cols.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var col in cols.EnumerateArray())
                            {
                                var colId = col.TryGetProperty("id", out var cid) ? cid.GetString() ?? "" : "";
                                var colText = col.TryGetProperty("text", out var ct) ? (ct.GetString() ?? string.Empty) : string.Empty;
                                if (!string.IsNullOrEmpty(colId))
                                {
                                    row[colId] = colText;
                                }
                            }
                        }

                        results.Add(row);
                    }
                }

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Monday: read {Count} items from board {BoardId} across {Pages} page(s).",
                results.Count, boardId, page);
            return results;
        }

        // ── Groups ───────────────────────────────────────────────────────────

        private async Task<List<object>> ReadGroupsAsync(string accessToken, string? boardId)
        {
            if (string.IsNullOrEmpty(boardId))
                throw new ConnectorException(
                    "Monday.com 'groups' resource requires the 'boardId' parameter.",
                    new ArgumentException("Missing 'boardId' parameter."),
                    "monday");

            var results = new List<object>();
            var query = $"{{ boards(ids:[{boardId}]) {{ groups {{ id title color archived deleted }} }} }}";
            var root = await ExecuteGraphQLAsync(accessToken, query);

            if (root.TryGetProperty("data", out var data)
                && data.TryGetProperty("boards", out var boards)
                && boards.ValueKind == JsonValueKind.Array)
            {
                var boardEl = boards.EnumerateArray().FirstOrDefault();
                if (boardEl.ValueKind != JsonValueKind.Undefined
                    && boardEl.TryGetProperty("groups", out var groups)
                    && groups.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonArray(groups, results);
                }
            }

            _logger.LogInformation("Monday: read {Count} groups from board {BoardId}.", results.Count, boardId);
            return results;
        }

        // ── Columns ──────────────────────────────────────────────────────────

        private async Task<List<object>> ReadColumnsAsync(string accessToken, string? boardId)
        {
            if (string.IsNullOrEmpty(boardId))
                throw new ConnectorException(
                    "Monday.com 'columns' resource requires the 'boardId' parameter.",
                    new ArgumentException("Missing 'boardId' parameter."),
                    "monday");

            var results = new List<object>();
            var query = $"{{ boards(ids:[{boardId}]) {{ columns {{ id title type settings_str }} }} }}";
            var root = await ExecuteGraphQLAsync(accessToken, query);

            if (root.TryGetProperty("data", out var data)
                && data.TryGetProperty("boards", out var boards)
                && boards.ValueKind == JsonValueKind.Array)
            {
                var boardEl = boards.EnumerateArray().FirstOrDefault();
                if (boardEl.ValueKind != JsonValueKind.Undefined
                    && boardEl.TryGetProperty("columns", out var columns)
                    && columns.ValueKind == JsonValueKind.Array)
                {
                    FlattenJsonArray(columns, results);
                }
            }

            _logger.LogInformation("Monday: read {Count} columns from board {BoardId}.", results.Count, boardId);
            return results;
        }

        // ── Users ────────────────────────────────────────────────────────────

        private async Task<List<object>> ReadUsersAsync(string accessToken, int maxPages)
        {
            var results = new List<object>();
            int page = 1;

            do
            {
                var query = $"{{ users(limit:{PageLimit}, page:{page}) {{ id name email enabled account {{ id name }} }} }}";
                var root = await ExecuteGraphQLAsync(accessToken, query);

                if (!root.TryGetProperty("data", out var data)
                    || !data.TryGetProperty("users", out var users)
                    || users.ValueKind != JsonValueKind.Array)
                    break;

                int count = 0;
                foreach (var element in users.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    foreach (var prop in element.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Object)
                        {
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
                    count++;
                }

                if (count < PageLimit) break;
                page++;
            }
            while (page <= maxPages);

            _logger.LogInformation("Monday: read {Count} users across {Pages} page(s).", results.Count, page);
            return results;
        }

        // ── Workspaces ───────────────────────────────────────────────────────

        private async Task<List<object>> ReadWorkspacesAsync(string accessToken, int maxPages)
        {
            var results = new List<object>();
            int page = 1;

            do
            {
                var query = $"{{ workspaces(limit:{PageLimit}, page:{page}) {{ id name kind description }} }}";
                var root = await ExecuteGraphQLAsync(accessToken, query);

                if (!root.TryGetProperty("data", out var data)
                    || !data.TryGetProperty("workspaces", out var workspaces)
                    || workspaces.ValueKind != JsonValueKind.Array)
                    break;

                int count = 0;
                FlattenJsonArray(workspaces, results);
                count = workspaces.GetArrayLength();

                if (count < PageLimit) break;
                page++;
            }
            while (page <= maxPages);

            _logger.LogInformation("Monday: read {Count} workspaces across {Pages} page(s).", results.Count, page);
            return results;
        }

        // ── Tags ─────────────────────────────────────────────────────────────

        private async Task<List<object>> ReadTagsAsync(string accessToken)
        {
            var results = new List<object>();
            var query = "{ tags { id name color } }";
            var root = await ExecuteGraphQLAsync(accessToken, query);

            if (root.TryGetProperty("data", out var data)
                && data.TryGetProperty("tags", out var tags)
                && tags.ValueKind == JsonValueKind.Array)
            {
                FlattenJsonArray(tags, results);
            }

            _logger.LogInformation("Monday: read {Count} tags.", results.Count);
            return results;
        }

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
                    $"Monday.com GraphQL error: {message}",
                    new InvalidOperationException(message ?? "Unknown GraphQL error"),
                    "monday");
            }

            return doc.RootElement.Clone();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void FlattenJsonArray(JsonElement array, List<object> results)
        {
            foreach (var element in array.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
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
            // Monday.com uses "Authorization: {apiKey}" (no Bearer prefix).
            request.Headers.TryAddWithoutValidation("Authorization", accessToken);
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
                    $"Monday.com connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "monday");
            return value;
        }
    }
}
