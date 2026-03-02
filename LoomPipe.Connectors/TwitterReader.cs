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
    /// Reads data from X (formerly Twitter) using the v2 API.
    ///
    /// Parameters:
    ///   accessToken  — Bearer token for X API v2
    ///   resource     — tweets, users, followers, following, likes, lists, spaces, bookmarks
    ///   userId       — X user ID (required for user-scoped resources)
    ///   query        — search query (for search resource)
    /// </summary>
    public class TwitterReader : ISourceReader
    {
        private const string BaseUrl = "https://api.twitter.com/2";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "tweets", "users", "followers", "following", "likes",
            "lists", "spaces", "bookmarks"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TwitterReader> _logger;

        public TwitterReader(HttpClient httpClient, ILogger<TwitterReader> logger)
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
                    "X (Twitter) Bearer token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "twitter");
            var userId = GetStringParam(config.Parameters, "userId");
            var query  = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Twitter: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, userId, query);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twitter: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Twitter resource '{resource}': {ex.Message}", ex, "twitter");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "X (Twitter) Bearer token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "twitter");
            var userId = GetStringParam(config.Parameters, "userId");
            var query  = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Twitter: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, userId, query, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twitter: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Twitter schema for '{resource}': {ex.Message}", ex, "twitter");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "X (Twitter) Bearer token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "twitter");
            var userId = GetStringParam(config.Parameters, "userId");
            var query  = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Twitter: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, userId, query, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twitter: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Twitter dry run preview failed for '{resource}': {ex.Message}", ex, "twitter");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? userId, string? query,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? paginationToken = null;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, userId, query, paginationToken);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, results);

                // Pagination — meta.next_token
                paginationToken = null;
                if (doc.RootElement.TryGetProperty("meta", out var meta)
                    && meta.TryGetProperty("next_token", out var nextEl)
                    && nextEl.ValueKind == JsonValueKind.String)
                {
                    paginationToken = nextEl.GetString();
                }

                page++;
            }
            while (paginationToken != null && page < maxPages);

            _logger.LogInformation("Twitter: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? userId, string? query, string? paginationToken)
        {
            var sb = new StringBuilder();
            var tweetFields = "created_at,public_metrics,author_id,conversation_id";
            var userFields  = "id,name,username,created_at,public_metrics,description,profile_image_url";

            switch (resource)
            {
                case "tweets":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'tweets' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/tweets");
                    sb.Append($"?max_results={PageLimit}&tweet.fields={tweetFields}");
                    break;

                case "users":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'users' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}");
                    sb.Append($"?user.fields={userFields}");
                    break;

                case "followers":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'followers' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/followers");
                    sb.Append($"?max_results={PageLimit}&user.fields={userFields}");
                    break;

                case "following":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'following' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/following");
                    sb.Append($"?max_results={PageLimit}&user.fields={userFields}");
                    break;

                case "likes":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'likes' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/liked_tweets");
                    sb.Append($"?max_results={PageLimit}&tweet.fields={tweetFields}");
                    break;

                case "lists":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'lists' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/owned_lists");
                    sb.Append($"?max_results={PageLimit}&list.fields=created_at,follower_count,member_count,description");
                    break;

                case "spaces":
                    sb.Append($"{BaseUrl}/spaces/search");
                    sb.Append($"?query={Uri.EscapeDataString(query ?? "live")}&state=live");
                    sb.Append($"&space.fields=id,state,title,host_ids,created_at,started_at,participant_count");
                    break;

                case "bookmarks":
                    if (string.IsNullOrEmpty(userId))
                        throw new ConnectorException(
                            "Twitter: 'userId' parameter is required for the 'bookmarks' resource.",
                            new ArgumentException("Missing 'userId'."),
                            "twitter");
                    sb.Append($"{BaseUrl}/users/{Uri.EscapeDataString(userId)}/bookmarks");
                    sb.Append($"?max_results={PageLimit}&tweet.fields={tweetFields}");
                    break;

                default:
                    // Support search as a special resource name
                    if (resource == "search" || resource.StartsWith("search"))
                    {
                        if (string.IsNullOrEmpty(query))
                            throw new ConnectorException(
                                "Twitter: 'query' parameter is required for the 'search' resource.",
                                new ArgumentException("Missing 'query'."),
                                "twitter");
                        sb.Append($"{BaseUrl}/tweets/search/recent");
                        sb.Append($"?query={Uri.EscapeDataString(query)}&max_results={PageLimit}&tweet.fields={tweetFields}");
                        break;
                    }
                    throw new ConnectorException(
                        $"Twitter: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "twitter");
            }

            if (!string.IsNullOrEmpty(paginationToken))
            {
                sb.Append($"&pagination_token={Uri.EscapeDataString(paginationToken)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("data", out items))
            {
                if (items.ValueKind == JsonValueKind.Array)
                {
                    // Standard v2 list response
                }
                else if (items.ValueKind == JsonValueKind.Object)
                {
                    // Single-object response (e.g., GET /users/{id}) — wrap in array
                    IDictionary<string, object> row = new ExpandoObject();
                    foreach (var prop in items.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                    results.Add(row);
                    return;
                }
                else
                {
                    return;
                }
            }
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
                    if (prop.Name == "public_metrics" && prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten public_metrics into the row
                        foreach (var metric in prop.Value.EnumerateObject())
                        {
                            row[metric.Name] = ConvertJsonValue(metric.Value);
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
                    $"Twitter connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "twitter");
            return value;
        }
    }
}
