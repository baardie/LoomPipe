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
    /// Reads data from Instagram using the Graph API (v19.0).
    ///
    /// Parameters:
    ///   accessToken  — Instagram/Facebook access token (long-lived preferred)
    ///   resource     — media, stories, insights, comments, hashtags, mentions
    ///   userId       — Instagram Business Account ID
    /// </summary>
    public class InstagramReader : ISourceReader
    {
        private const string BaseUrl = "https://graph.facebook.com/v19.0";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "media", "stories", "insights", "comments", "hashtags", "mentions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<InstagramReader> _logger;

        public InstagramReader(HttpClient httpClient, ILogger<InstagramReader> logger)
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
                    "Instagram access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "instagram");
            var userId = GetRequiredParam(config.Parameters, "userId");

            _logger.LogInformation("Instagram: reading resource '{Resource}' for user '{UserId}'.", resource, userId);

            try
            {
                return await ReadFullAsync(resource, accessToken, userId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Instagram: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Instagram resource '{resource}': {ex.Message}", ex, "instagram");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Instagram access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "instagram");
            var userId = GetRequiredParam(config.Parameters, "userId");

            _logger.LogInformation("Instagram: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, userId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Instagram: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Instagram schema for '{resource}': {ex.Message}", ex, "instagram");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Instagram access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "instagram");
            var userId = GetRequiredParam(config.Parameters, "userId");

            _logger.LogInformation("Instagram: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, userId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Instagram: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Instagram dry run preview failed for '{resource}': {ex.Message}", ex, "instagram");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string userId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, accessToken, userId, cursor);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, results);

                // Pagination — paging.cursors.after
                cursor = null;
                if (doc.RootElement.TryGetProperty("paging", out var paging)
                    && paging.TryGetProperty("cursors", out var cursors)
                    && cursors.TryGetProperty("after", out var afterEl)
                    && afterEl.ValueKind == JsonValueKind.String)
                {
                    cursor = afterEl.GetString();
                }

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Instagram: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string accessToken, string userId, string? cursor)
        {
            var sb = new StringBuilder();

            switch (resource)
            {
                case "media":
                    sb.Append($"{BaseUrl}/{userId}/media");
                    sb.Append($"?fields=id,caption,media_type,media_url,timestamp,like_count,comments_count");
                    sb.Append($"&limit={PageLimit}");
                    break;

                case "stories":
                    sb.Append($"{BaseUrl}/{userId}/stories");
                    sb.Append($"?fields=id,media_type,media_url,timestamp");
                    sb.Append($"&limit={PageLimit}");
                    break;

                case "insights":
                    sb.Append($"{BaseUrl}/{userId}/insights");
                    sb.Append($"?metric=impressions,reach,profile_views&period=day");
                    break;

                case "comments":
                    // Comments on media — requires fetching media first, but for simplicity
                    // we read recent media comments via the user media edge.
                    sb.Append($"{BaseUrl}/{userId}/media");
                    sb.Append($"?fields=id,comments{{id,text,username,timestamp}}");
                    sb.Append($"&limit={PageLimit}");
                    break;

                case "hashtags":
                    sb.Append($"{BaseUrl}/{userId}/recently_searched_hashtags");
                    sb.Append($"?fields=id,name");
                    break;

                case "mentions":
                    sb.Append($"{BaseUrl}/{userId}/tags");
                    sb.Append($"?fields=id,caption,media_type,media_url,timestamp");
                    sb.Append($"&limit={PageLimit}");
                    break;

                default:
                    throw new ConnectorException(
                        $"Instagram: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "instagram");
            }

            // Append access_token as query param (Graph API standard)
            sb.Append($"&access_token={Uri.EscapeDataString(accessToken)}");

            if (!string.IsNullOrEmpty(cursor))
            {
                sb.Append($"&after={Uri.EscapeDataString(cursor)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Graph API response shape
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
                    $"Instagram connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "instagram");
            return value;
        }
    }
}
