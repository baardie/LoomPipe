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
    /// Reads data from YouTube using the Data API v3.
    ///
    /// Parameters:
    ///   accessToken  — API key (key= query param) or OAuth token (Bearer header)
    ///   resource     — channels, videos, playlists, playlist_items, comments,
    ///                  subscriptions, search, captions, activities
    ///   channelId    — optional YouTube channel ID
    ///   playlistId   — optional playlist ID (for playlist_items)
    /// </summary>
    public class YouTubeReader : ISourceReader
    {
        private const string BaseUrl = "https://www.googleapis.com/youtube/v3";
        private const int PageLimit = 50;

        private static readonly string[] AllResources =
        {
            "channels", "videos", "playlists", "playlist_items", "comments",
            "subscriptions", "search", "captions", "activities"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<YouTubeReader> _logger;

        public YouTubeReader(HttpClient httpClient, ILogger<YouTubeReader> logger)
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
                    "YouTube API key or OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "youtube");
            var channelId  = GetStringParam(config.Parameters, "channelId");
            var playlistId = GetStringParam(config.Parameters, "playlistId");

            _logger.LogInformation("YouTube: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, channelId, playlistId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "YouTube: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read YouTube resource '{resource}': {ex.Message}", ex, "youtube");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "YouTube API key or OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "youtube");
            var channelId  = GetStringParam(config.Parameters, "channelId");
            var playlistId = GetStringParam(config.Parameters, "playlistId");

            _logger.LogInformation("YouTube: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, channelId, playlistId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "YouTube: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover YouTube schema for '{resource}': {ex.Message}", ex, "youtube");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "YouTube API key or OAuth token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "youtube");
            var channelId  = GetStringParam(config.Parameters, "channelId");
            var playlistId = GetStringParam(config.Parameters, "playlistId");

            _logger.LogInformation("YouTube: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, channelId, playlistId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "YouTube: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"YouTube dry run preview failed for '{resource}': {ex.Message}", ex, "youtube");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? channelId, string? playlistId,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? pageToken = null;
            int page = 0;
            bool isApiKey = IsApiKey(accessToken);

            do
            {
                var url = BuildUrl(resource, accessToken, channelId, playlistId, pageToken, isApiKey);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                if (!isApiKey)
                {
                    ApplyAuth(request, accessToken);
                }

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, results);

                // Pagination — nextPageToken
                pageToken = null;
                if (doc.RootElement.TryGetProperty("nextPageToken", out var nextEl)
                    && nextEl.ValueKind == JsonValueKind.String)
                {
                    pageToken = nextEl.GetString();
                }

                page++;
            }
            while (pageToken != null && page < maxPages);

            _logger.LogInformation("YouTube: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(
            string resource, string accessToken, string? channelId, string? playlistId,
            string? pageToken, bool isApiKey)
        {
            var sb = new StringBuilder();

            switch (resource)
            {
                case "videos":
                    sb.Append($"{BaseUrl}/videos");
                    sb.Append($"?part=snippet,statistics,contentDetails&chart=mostPopular&maxResults={PageLimit}");
                    break;

                case "channels":
                    sb.Append($"{BaseUrl}/channels");
                    sb.Append($"?part=snippet,statistics,contentDetails");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&id={Uri.EscapeDataString(channelId)}");
                    else
                        sb.Append("&mine=true");
                    break;

                case "playlists":
                    sb.Append($"{BaseUrl}/playlists");
                    sb.Append($"?part=snippet,contentDetails&maxResults={PageLimit}");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&channelId={Uri.EscapeDataString(channelId)}");
                    else
                        sb.Append("&mine=true");
                    break;

                case "playlist_items":
                    sb.Append($"{BaseUrl}/playlistItems");
                    sb.Append($"?part=snippet,contentDetails&maxResults={PageLimit}");
                    if (!string.IsNullOrEmpty(playlistId))
                        sb.Append($"&playlistId={Uri.EscapeDataString(playlistId)}");
                    break;

                case "comments":
                    sb.Append($"{BaseUrl}/commentThreads");
                    sb.Append($"?part=snippet,replies&maxResults={PageLimit}");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&allThreadsRelatedToChannelId={Uri.EscapeDataString(channelId)}");
                    break;

                case "subscriptions":
                    sb.Append($"{BaseUrl}/subscriptions");
                    sb.Append($"?part=snippet,contentDetails&maxResults={PageLimit}");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&channelId={Uri.EscapeDataString(channelId)}");
                    else
                        sb.Append("&mine=true");
                    break;

                case "search":
                    sb.Append($"{BaseUrl}/search");
                    sb.Append($"?part=snippet&maxResults={PageLimit}&type=video");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&channelId={Uri.EscapeDataString(channelId)}");
                    break;

                case "captions":
                    sb.Append($"{BaseUrl}/captions");
                    sb.Append($"?part=snippet");
                    break;

                case "activities":
                    sb.Append($"{BaseUrl}/activities");
                    sb.Append($"?part=snippet,contentDetails&maxResults={PageLimit}");
                    if (!string.IsNullOrEmpty(channelId))
                        sb.Append($"&channelId={Uri.EscapeDataString(channelId)}");
                    else
                        sb.Append("&mine=true");
                    break;

                default:
                    throw new ConnectorException(
                        $"YouTube: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "youtube");
            }

            // Auth — API key as query param or OAuth token via header (handled in caller)
            if (isApiKey)
            {
                sb.Append($"&key={Uri.EscapeDataString(accessToken)}");
            }

            if (!string.IsNullOrEmpty(pageToken))
            {
                sb.Append($"&pageToken={Uri.EscapeDataString(pageToken)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("items", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard YouTube API response shape
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

                // Top-level fields
                if (element.TryGetProperty("id", out var id))
                {
                    if (id.ValueKind == JsonValueKind.String)
                        row["id"] = id.GetString() ?? string.Empty;
                    else if (id.ValueKind == JsonValueKind.Object)
                    {
                        // Search results have id as an object: { videoId, channelId, playlistId }
                        foreach (var idProp in id.EnumerateObject())
                        {
                            row[idProp.Name] = ConvertJsonValue(idProp.Value);
                        }
                    }
                    else
                        row["id"] = id.ToString();
                }

                if (element.TryGetProperty("kind", out var kind))
                    row["kind"] = kind.GetString() ?? kind.ToString();

                if (element.TryGetProperty("etag", out var etag))
                    row["etag"] = etag.GetString() ?? etag.ToString();

                // Flatten "snippet" sub-object
                if (element.TryGetProperty("snippet", out var snippet) && snippet.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in snippet.EnumerateObject())
                    {
                        row[$"snippet_{prop.Name}"] = ConvertJsonValue(prop.Value);
                    }
                }

                // Flatten "statistics" sub-object
                if (element.TryGetProperty("statistics", out var stats) && stats.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in stats.EnumerateObject())
                    {
                        row[$"statistics_{prop.Name}"] = ConvertJsonValue(prop.Value);
                    }
                }

                // Flatten "contentDetails" sub-object
                if (element.TryGetProperty("contentDetails", out var cd) && cd.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in cd.EnumerateObject())
                    {
                        row[$"contentDetails_{prop.Name}"] = ConvertJsonValue(prop.Value);
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

        /// <summary>
        /// Heuristic: API keys are typically 39 chars starting with "AIza"; OAuth tokens are longer.
        /// </summary>
        private static bool IsApiKey(string token)
        {
            return token.StartsWith("AIza", StringComparison.Ordinal) && token.Length < 60;
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
                    $"YouTube connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "youtube");
            return value;
        }
    }
}
