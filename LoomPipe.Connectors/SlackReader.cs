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
    /// Reads data from the Slack Web API.
    ///
    /// Parameters:
    ///   accessToken  — Slack Bot token (xoxb-...) or User token (xoxp-...)
    ///   resource     — channels, messages, users, files, reactions, team, usergroups
    ///   channel      — channel ID (required for messages resource)
    ///   startDate    — optional Unix timestamp for filtering messages (oldest param)
    /// </summary>
    public class SlackReader : ISourceReader
    {
        private const string BaseUrl = "https://slack.com/api";
        private const int PageLimit = 200;

        private static readonly string[] AllResources =
        {
            "channels", "messages", "users", "files",
            "reactions", "team", "usergroups"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SlackReader> _logger;

        public SlackReader(HttpClient httpClient, ILogger<SlackReader> logger)
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
                    "Slack access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "slack");

            _logger.LogInformation("Slack: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(config, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Slack: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Slack resource '{resource}': {ex.Message}", ex, "slack");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Slack access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "slack");

            _logger.LogInformation("Slack: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Slack: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Slack schema for '{resource}': {ex.Message}", ex, "slack");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Slack access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "slack");

            _logger.LogInformation("Slack: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(config, resource, accessToken, maxPages: 1, limit: sampleSize);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Slack: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Slack dry run preview failed for '{resource}': {ex.Message}", ex, "slack");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            DataSourceConfig config, string resource, string accessToken,
            int maxPages = int.MaxValue, int? limit = null)
        {
            var results = new List<object>();
            string? cursor = null;
            int page = 0;
            int pageLimit = limit ?? PageLimit;

            do
            {
                var url = BuildApiUrl(config, resource, pageLimit, cursor);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                // Slack wraps errors in ok=false
                if (root.TryGetProperty("ok", out var ok) && !ok.GetBoolean())
                {
                    var error = root.TryGetProperty("error", out var errEl) ? errEl.GetString() : "unknown";
                    throw new ConnectorException(
                        $"Slack API error for '{resource}': {error}",
                        new HttpRequestException($"Slack API returned ok=false: {error}"),
                        "slack");
                }

                ParseResultsPage(root, resource, results);

                // Cursor-based pagination
                cursor = null;
                if (root.TryGetProperty("response_metadata", out var meta)
                    && meta.TryGetProperty("next_cursor", out var nextCursor)
                    && nextCursor.ValueKind == JsonValueKind.String)
                {
                    var cursorValue = nextCursor.GetString();
                    if (!string.IsNullOrEmpty(cursorValue))
                    {
                        cursor = cursorValue;
                    }
                }

                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Slack: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildApiUrl(DataSourceConfig config, string resource, int limit, string? cursor)
        {
            var sb = new StringBuilder();

            switch (resource)
            {
                case "channels":
                    sb.Append($"{BaseUrl}/conversations.list?limit={limit}&types=public_channel,private_channel");
                    break;

                case "messages":
                    var channel = GetRequiredParam(config.Parameters, "channel");
                    sb.Append($"{BaseUrl}/conversations.history?channel={Uri.EscapeDataString(channel)}&limit={limit}");
                    var startDate = GetStringParam(config.Parameters, "startDate");
                    if (!string.IsNullOrEmpty(startDate))
                    {
                        sb.Append($"&oldest={Uri.EscapeDataString(startDate)}");
                    }
                    break;

                case "users":
                    sb.Append($"{BaseUrl}/users.list?limit={limit}");
                    break;

                case "files":
                    sb.Append($"{BaseUrl}/files.list?count={limit}");
                    break;

                case "reactions":
                    sb.Append($"{BaseUrl}/reactions.list?limit={limit}");
                    break;

                case "team":
                    sb.Append($"{BaseUrl}/team.info?");
                    break;

                case "usergroups":
                    sb.Append($"{BaseUrl}/usergroups.list?include_users=true");
                    break;

                default:
                    throw new ConnectorException(
                        $"Unsupported Slack resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "slack");
            }

            if (!string.IsNullOrEmpty(cursor))
            {
                sb.Append($"&cursor={Uri.EscapeDataString(cursor)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            // Each Slack API method wraps results in a resource-specific key.
            var arrayPropertyName = resource switch
            {
                "channels"   => "channels",
                "messages"   => "messages",
                "users"      => "members",
                "files"      => "files",
                "reactions"  => "items",
                "usergroups" => "usergroups",
                _            => null
            };

            // team.info returns a single object under "team"
            if (resource == "team")
            {
                if (root.TryGetProperty("team", out var teamEl) && teamEl.ValueKind == JsonValueKind.Object)
                {
                    results.Add(FlattenJsonObject(teamEl));
                }
                return;
            }

            if (arrayPropertyName == null) return;

            if (!root.TryGetProperty(arrayPropertyName, out var items) || items.ValueKind != JsonValueKind.Array)
                return;

            foreach (var element in items.EnumerateArray())
            {
                results.Add(FlattenJsonObject(element));
            }
        }

        private static object FlattenJsonObject(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (element.ValueKind != JsonValueKind.Object) return row;

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
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
                    $"Slack connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "slack");
            return value;
        }
    }
}
