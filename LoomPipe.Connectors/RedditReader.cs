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
    /// Reads data from Reddit using the OAuth API.
    ///
    /// Parameters:
    ///   accessToken  — Reddit OAuth2 Bearer token
    ///   resource     — posts, comments, subreddits, users, search, modlog
    ///   subreddit    — subreddit name (without r/ prefix)
    ///   sort         — hot, new, top (default: hot)
    ///   timeframe    — hour, day, week, month, year, all (for top sort; default: all)
    /// </summary>
    public class RedditReader : ISourceReader
    {
        private const string BaseUrl = "https://oauth.reddit.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "posts", "comments", "subreddits", "users", "search", "modlog"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<RedditReader> _logger;

        public RedditReader(HttpClient httpClient, ILogger<RedditReader> logger)
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
                    "Reddit OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "reddit");
            var subreddit = GetStringParam(config.Parameters, "subreddit");
            var sort      = GetStringParam(config.Parameters, "sort") ?? "hot";
            var timeframe = GetStringParam(config.Parameters, "timeframe") ?? "all";

            _logger.LogInformation("Reddit: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, subreddit, sort, timeframe);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Reddit: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Reddit resource '{resource}': {ex.Message}", ex, "reddit");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Reddit OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "reddit");
            var subreddit = GetStringParam(config.Parameters, "subreddit");
            var sort      = GetStringParam(config.Parameters, "sort") ?? "hot";
            var timeframe = GetStringParam(config.Parameters, "timeframe") ?? "all";

            _logger.LogInformation("Reddit: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, subreddit, sort, timeframe, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Reddit: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Reddit schema for '{resource}': {ex.Message}", ex, "reddit");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Reddit OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "reddit");
            var subreddit = GetStringParam(config.Parameters, "subreddit");
            var sort      = GetStringParam(config.Parameters, "sort") ?? "hot";
            var timeframe = GetStringParam(config.Parameters, "timeframe") ?? "all";

            _logger.LogInformation("Reddit: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, subreddit, sort, timeframe, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Reddit: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Reddit dry run preview failed for '{resource}': {ex.Message}", ex, "reddit");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? subreddit, string sort, string timeframe,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? after = null;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, subreddit, sort, timeframe, after);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                after = ParseResultsPage(doc.RootElement, results);

                page++;
            }
            while (after != null && page < maxPages);

            _logger.LogInformation("Reddit: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? subreddit, string sort, string timeframe, string? after)
        {
            var sb = new StringBuilder();

            switch (resource)
            {
                case "posts":
                    if (string.IsNullOrEmpty(subreddit))
                        throw new ConnectorException(
                            "Reddit: 'subreddit' parameter is required for the 'posts' resource.",
                            new ArgumentException("Missing 'subreddit'."),
                            "reddit");
                    sb.Append($"{BaseUrl}/r/{Uri.EscapeDataString(subreddit)}/{Uri.EscapeDataString(sort)}");
                    sb.Append($"?limit={PageLimit}");
                    if (sort == "top")
                        sb.Append($"&t={Uri.EscapeDataString(timeframe)}");
                    break;

                case "comments":
                    if (string.IsNullOrEmpty(subreddit))
                        throw new ConnectorException(
                            "Reddit: 'subreddit' parameter is required for the 'comments' resource.",
                            new ArgumentException("Missing 'subreddit'."),
                            "reddit");
                    sb.Append($"{BaseUrl}/r/{Uri.EscapeDataString(subreddit)}/comments");
                    sb.Append($"?limit={PageLimit}");
                    break;

                case "subreddits":
                    sb.Append($"{BaseUrl}/subreddits/popular");
                    sb.Append($"?limit={PageLimit}");
                    break;

                case "users":
                    sb.Append($"{BaseUrl}/users/popular");
                    sb.Append($"?limit={PageLimit}");
                    break;

                case "search":
                    if (string.IsNullOrEmpty(subreddit))
                    {
                        sb.Append($"{BaseUrl}/search");
                        sb.Append($"?limit={PageLimit}&sort={Uri.EscapeDataString(sort)}");
                    }
                    else
                    {
                        sb.Append($"{BaseUrl}/r/{Uri.EscapeDataString(subreddit)}/search");
                        sb.Append($"?restrict_sr=on&limit={PageLimit}&sort={Uri.EscapeDataString(sort)}");
                    }
                    if (sort == "top")
                        sb.Append($"&t={Uri.EscapeDataString(timeframe)}");
                    break;

                case "modlog":
                    if (string.IsNullOrEmpty(subreddit))
                        throw new ConnectorException(
                            "Reddit: 'subreddit' parameter is required for the 'modlog' resource.",
                            new ArgumentException("Missing 'subreddit'."),
                            "reddit");
                    sb.Append($"{BaseUrl}/r/{Uri.EscapeDataString(subreddit)}/about/log");
                    sb.Append($"?limit={PageLimit}");
                    break;

                default:
                    throw new ConnectorException(
                        $"Reddit: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "reddit");
            }

            if (!string.IsNullOrEmpty(after))
            {
                sb.Append($"&after={Uri.EscapeDataString(after)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a Reddit listing response. Returns the "after" fullname for pagination, or null.
        /// Reddit responses follow: { "data": { "children": [{ "data": {...} }], "after": "t3_xxx" } }
        /// </summary>
        private static string? ParseResultsPage(JsonElement root, List<object> results)
        {
            string? after = null;

            if (!root.TryGetProperty("data", out var data))
            {
                // Some endpoints may return a flat array
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in root.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                    }
                }
                return null;
            }

            // Extract pagination cursor
            if (data.TryGetProperty("after", out var afterEl)
                && afterEl.ValueKind == JsonValueKind.String)
            {
                after = afterEl.GetString();
            }

            if (!data.TryGetProperty("children", out var children)
                || children.ValueKind != JsonValueKind.Array)
            {
                return after;
            }

            foreach (var child in children.EnumerateArray())
            {
                // Each child has { "kind": "t3", "data": { ... } }
                if (!child.TryGetProperty("data", out var childData)
                    || childData.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in childData.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                // Include the kind (t1=comment, t3=post, t5=subreddit, etc.)
                if (child.TryGetProperty("kind", out var kind))
                {
                    row["kind"] = kind.GetString() ?? kind.ToString();
                }

                results.Add(row);
            }

            return after;
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
            // Reddit requires a User-Agent header
            request.Headers.UserAgent.ParseAdd("LoomPipe/1.0 (connector)");
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
                    $"Reddit connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "reddit");
            return value;
        }
    }
}
