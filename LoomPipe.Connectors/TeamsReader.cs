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
    /// Reads data from Microsoft Teams via the Microsoft Graph API.
    ///
    /// Parameters:
    ///   accessToken  — Microsoft Graph application access token (Bearer)
    ///   resource     — teams, channels, messages, users, groups, chats
    ///   teamId       — team ID (required for channels and messages)
    ///   channelId    — channel ID (required for messages)
    /// </summary>
    public class TeamsReader : ISourceReader
    {
        private const string BaseUrl = "https://graph.microsoft.com/v1.0";
        private const int PageLimit = 50;

        private static readonly string[] AllResources =
        {
            "teams", "channels", "messages", "users", "groups", "chats"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TeamsReader> _logger;

        public TeamsReader(HttpClient httpClient, ILogger<TeamsReader> logger)
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
                    "Microsoft Graph access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "teams");

            _logger.LogInformation("Teams: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(config, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Teams: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Teams resource '{resource}': {ex.Message}", ex, "teams");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Microsoft Graph access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "teams");

            _logger.LogInformation("Teams: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Teams: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Teams schema for '{resource}': {ex.Message}", ex, "teams");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Microsoft Graph access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "teams");

            _logger.LogInformation("Teams: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(config, resource, accessToken, maxPages: 1, top: sampleSize);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Teams: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Teams dry run preview failed for '{resource}': {ex.Message}", ex, "teams");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            DataSourceConfig config, string resource, string accessToken,
            int maxPages = int.MaxValue, int? top = null)
        {
            var results = new List<object>();
            var url = BuildApiUrl(config, resource, top ?? PageLimit);
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                ParseResultsPage(root, results);

                // OData pagination via @odata.nextLink
                url = null;
                if (root.TryGetProperty("@odata.nextLink", out var nextLink)
                    && nextLink.ValueKind == JsonValueKind.String)
                {
                    url = nextLink.GetString();
                }

                page++;
            }
            while (url != null && page < maxPages);

            _logger.LogInformation("Teams: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildApiUrl(DataSourceConfig config, string resource, int top)
        {
            switch (resource)
            {
                case "teams":
                    return $"{BaseUrl}/teams?$top={top}";

                case "channels":
                {
                    var teamId = GetRequiredParam(config.Parameters, "teamId");
                    return $"{BaseUrl}/teams/{Uri.EscapeDataString(teamId)}/channels";
                }

                case "messages":
                {
                    var teamId = GetRequiredParam(config.Parameters, "teamId");
                    var channelId = GetRequiredParam(config.Parameters, "channelId");
                    return $"{BaseUrl}/teams/{Uri.EscapeDataString(teamId)}/channels/{Uri.EscapeDataString(channelId)}/messages?$top={top}";
                }

                case "users":
                    return $"{BaseUrl}/users?$top={top}";

                case "groups":
                    return $"{BaseUrl}/groups?$top={top}";

                case "chats":
                    return $"{BaseUrl}/chats?$top={top}";

                default:
                    throw new ConnectorException(
                        $"Unsupported Teams resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "teams");
            }
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            // Graph API wraps results in "value" array
            if (root.TryGetProperty("value", out var items) && items.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in items.EnumerateArray())
                {
                    results.Add(FlattenJsonObject(element));
                }
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in root.EnumerateArray())
                {
                    results.Add(FlattenJsonObject(element));
                }
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
                    $"Teams connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "teams");
            return value;
        }
    }
}
