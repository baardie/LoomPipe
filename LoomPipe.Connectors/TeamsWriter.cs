#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
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
    /// Writes messages to a Microsoft Teams channel via the Graph API.
    ///
    /// Parameters:
    ///   accessToken  — Microsoft Graph application access token (Bearer)
    ///   teamId       — target team ID
    ///   channelId    — target channel ID
    ///
    /// Each row becomes a channel message. If the row contains a "content" or "body" field
    /// it is used as the message HTML body; otherwise the entire row is serialised as JSON.
    /// </summary>
    public class TeamsWriter : IDestinationWriter
    {
        private const string BaseUrl = "https://graph.microsoft.com/v1.0";
        private const int MaxRetries = 3;

        private readonly HttpClient _httpClient;
        private readonly ILogger<TeamsWriter> _logger;

        public TeamsWriter(HttpClient httpClient, ILogger<TeamsWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── IDestinationWriter ────────────────────────────────────────────────

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Microsoft Graph access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "teams");
            var teamId    = GetRequiredParam(config.Parameters, "teamId");
            var channelId = GetRequiredParam(config.Parameters, "channelId");

            var url = $"{BaseUrl}/teams/{Uri.EscapeDataString(teamId)}/channels/{Uri.EscapeDataString(channelId)}/messages";

            _logger.LogInformation(
                "Teams: writing messages to team '{TeamId}' channel '{ChannelId}'.",
                teamId, channelId);

            try
            {
                var count = 0;
                foreach (var record in records)
                {
                    var content = ExtractContent(record);
                    await PostMessageAsync(accessToken, url, content);
                    count++;
                }

                _logger.LogInformation(
                    "Teams: successfully wrote {Count} messages to team '{TeamId}' channel '{ChannelId}'.",
                    count, teamId, channelId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Teams: failed to write messages to team '{TeamId}' channel '{ChannelId}'.",
                    teamId, channelId);
                throw new ConnectorException(
                    $"Failed to write Teams messages to team '{teamId}' channel '{channelId}': {ex.Message}",
                    ex, "teams");
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Microsoft Graph access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "teams");
            var teamId    = GetRequiredParam(config.Parameters, "teamId");
            var channelId = GetRequiredParam(config.Parameters, "channelId");

            _logger.LogInformation(
                "Teams: validating channel '{ChannelId}' in team '{TeamId}'.",
                channelId, teamId);

            try
            {
                var url = $"{BaseUrl}/teams/{Uri.EscapeDataString(teamId)}/channels/{Uri.EscapeDataString(channelId)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    return true;
                }

                _logger.LogWarning(
                    "Teams: channel validation failed with HTTP {StatusCode}.",
                    (int)response.StatusCode);
                return false;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Teams: failed to validate channel '{ChannelId}' in team '{TeamId}'.",
                    channelId, teamId);
                throw new ConnectorException(
                    $"Failed to validate Teams channel '{channelId}' in team '{teamId}': {ex.Message}",
                    ex, "teams");
            }
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            var teamId    = GetStringParam(config.Parameters, "teamId") ?? "unknown";
            var channelId = GetStringParam(config.Parameters, "channelId") ?? "unknown";

            _logger.LogInformation(
                "Teams: dry run preview for writer (team={TeamId}, channel={ChannelId}, sampleSize={SampleSize}).",
                teamId, channelId, sampleSize);

            var preview = records.Take(sampleSize).Select(record =>
            {
                IDictionary<string, object> expando = new ExpandoObject();
                expando["_teams_endpoint"] = $"POST /teams/{teamId}/channels/{channelId}/messages";
                expando["_teams_teamId"] = teamId;
                expando["_teams_channelId"] = channelId;
                expando["_teams_content"] = ExtractContent(record);
                return (object)expando;
            });

            return Task.FromResult(preview);
        }

        // ── Post message with retry ──────────────────────────────────────────

        private async Task PostMessageAsync(string accessToken, string url, string content)
        {
            for (var attempt = 0; attempt <= MaxRetries; attempt++)
            {
                var body = JsonSerializer.Serialize(new
                {
                    body = new
                    {
                        contentType = "html",
                        content
                    }
                });

                using var request = new HttpRequestMessage(HttpMethod.Post, url);
                ApplyAuth(request, accessToken);
                request.Content = new StringContent(body, Encoding.UTF8, "application/json");

                using var response = await _httpClient.SendAsync(request);

                // Handle rate limiting (HTTP 429)
                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Teams rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException("429 Too Many Requests"),
                            "teams");
                    }

                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(2);
                    _logger.LogWarning(
                        "Teams rate limit hit. Retrying in {Seconds}s (attempt {Attempt}/{Max}).",
                        retryAfter.TotalSeconds, attempt + 1, MaxRetries);
                    await Task.Delay(retryAfter);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    throw new ConnectorException(
                        $"Teams Graph API returned {(int)response.StatusCode}: {errorBody}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode}"),
                        "teams");
                }

                return; // Success
            }
        }

        // ── Content extraction ───────────────────────────────────────────────

        private static string ExtractContent(object record)
        {
            if (record is IDictionary<string, object> dict)
            {
                // Prefer "content" field, then "body"
                if (dict.TryGetValue("content", out var contentVal) && contentVal is string content && !string.IsNullOrEmpty(content))
                {
                    return content;
                }

                if (dict.TryGetValue("body", out var bodyVal) && bodyVal is string body && !string.IsNullOrEmpty(body))
                {
                    return body;
                }

                // Serialize the entire row as JSON fallback
                return JsonSerializer.Serialize(dict);
            }

            return record?.ToString() ?? string.Empty;
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
