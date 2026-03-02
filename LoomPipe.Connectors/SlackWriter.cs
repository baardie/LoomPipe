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
    /// Writes messages to a Slack channel via the chat.postMessage API.
    ///
    /// Parameters:
    ///   accessToken  — Slack Bot token (xoxb-...) with chat:write scope
    ///   channel      — target channel ID (e.g. C0123456789)
    ///
    /// Each row becomes a message. If the row contains a "text" field it is used as
    /// the message body; otherwise the entire row is serialised as JSON.
    /// </summary>
    public class SlackWriter : IDestinationWriter
    {
        private const string BaseUrl = "https://slack.com/api";
        private const int MaxRetries = 3;

        private readonly HttpClient _httpClient;
        private readonly ILogger<SlackWriter> _logger;

        public SlackWriter(HttpClient httpClient, ILogger<SlackWriter> logger)
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
                    "Slack access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "slack");
            var channel = GetRequiredParam(config.Parameters, "channel");

            _logger.LogInformation("Slack: writing messages to channel '{Channel}'.", channel);

            try
            {
                var count = 0;
                foreach (var record in records)
                {
                    var text = ExtractText(record);
                    await PostMessageAsync(accessToken, channel, text);
                    count++;
                }

                _logger.LogInformation(
                    "Slack: successfully wrote {Count} messages to channel '{Channel}'.",
                    count, channel);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Slack: failed to write messages to channel '{Channel}'.", channel);
                throw new ConnectorException($"Failed to write Slack messages to channel '{channel}': {ex.Message}", ex, "slack");
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Slack access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "slack");
            var channel = GetRequiredParam(config.Parameters, "channel");

            _logger.LogInformation("Slack: validating channel '{Channel}' exists.", channel);

            try
            {
                var url = $"{BaseUrl}/conversations.info?channel={Uri.EscapeDataString(channel)}";
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("ok", out var ok) && ok.GetBoolean())
                {
                    return true;
                }

                var error = root.TryGetProperty("error", out var errEl) ? errEl.GetString() : "unknown";
                _logger.LogWarning("Slack: channel validation failed — {Error}.", error);
                return false;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Slack: failed to validate channel '{Channel}'.", channel);
                throw new ConnectorException($"Failed to validate Slack channel '{channel}': {ex.Message}", ex, "slack");
            }
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            var channel = GetStringParam(config.Parameters, "channel") ?? "unknown";

            _logger.LogInformation(
                "Slack: dry run preview for writer (channel={Channel}, sampleSize={SampleSize}).",
                channel, sampleSize);

            var preview = records.Take(sampleSize).Select(record =>
            {
                IDictionary<string, object> expando = new ExpandoObject();
                expando["_slack_endpoint"] = "POST /api/chat.postMessage";
                expando["_slack_channel"] = channel;
                expando["_slack_text"] = ExtractText(record);
                return (object)expando;
            });

            return Task.FromResult(preview);
        }

        // ── Post message with retry ──────────────────────────────────────────

        private async Task PostMessageAsync(string accessToken, string channel, string text)
        {
            var url = $"{BaseUrl}/chat.postMessage";

            for (var attempt = 0; attempt <= MaxRetries; attempt++)
            {
                var body = JsonSerializer.Serialize(new { channel, text });

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
                            $"Slack rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException("429 Too Many Requests"),
                            "slack");
                    }

                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(2);
                    _logger.LogWarning(
                        "Slack rate limit hit. Retrying in {Seconds}s (attempt {Attempt}/{Max}).",
                        retryAfter.TotalSeconds, attempt + 1, MaxRetries);
                    await Task.Delay(retryAfter);
                    continue;
                }

                response.EnsureSuccessStatusCode();

                // Slack can return 200 with ok=false
                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty("ok", out var ok) && !ok.GetBoolean())
                {
                    var error = root.TryGetProperty("error", out var errEl) ? errEl.GetString() : "unknown";
                    throw new ConnectorException(
                        $"Slack chat.postMessage failed: {error}",
                        new HttpRequestException($"Slack API returned ok=false: {error}"),
                        "slack");
                }

                return; // Success
            }
        }

        // ── Text extraction ──────────────────────────────────────────────────

        private static string ExtractText(object record)
        {
            if (record is IDictionary<string, object> dict)
            {
                if (dict.TryGetValue("text", out var textVal) && textVal is string text && !string.IsNullOrEmpty(text))
                {
                    return text;
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
                    $"Slack connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "slack");
            return value;
        }
    }
}
