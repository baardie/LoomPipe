#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from the Pipedrive API v1.
    ///
    /// Parameters:
    ///   accessToken  — Pipedrive API token (passed as api_token query parameter)
    ///   resource     — deals, persons, organizations, activities, leads, products,
    ///                  pipelines, stages, users, notes, files, filters, goals, webhooks
    ///
    /// ConnectionString: plain API token string
    /// </summary>
    public class PipedriveReader : ISourceReader
    {
        private const string BaseUrl = "https://api.pipedrive.com/v1";
        private const int PageLimit = 100;
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "deals", "persons", "organizations", "activities", "leads",
            "products", "pipelines", "stages", "users", "notes",
            "files", "filters", "goals", "webhooks"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<PipedriveReader> _logger;

        public PipedriveReader(HttpClient httpClient, ILogger<PipedriveReader> logger)
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
                    "Pipedrive API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pipedrive");

            _logger.LogInformation("Pipedrive: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipedrive: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Failed to read Pipedrive resource '{resource}': {ex.Message}", ex, "pipedrive");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pipedrive API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pipedrive");

            _logger.LogInformation("Pipedrive: discovering schema for '{Resource}'.", resource);

            try
            {
                // Fetch a single page and inspect the first record's keys.
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipedrive: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Failed to discover Pipedrive schema for '{resource}': {ex.Message}", ex, "pipedrive");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pipedrive API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pipedrive");

            _logger.LogInformation("Pipedrive: dry run preview for '{Resource}' (sample={SampleSize}).",
                resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipedrive: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException(
                    $"Pipedrive dry run preview failed for '{resource}': {ex.Message}", ex, "pipedrive");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (offset-based pagination) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int start = 0;
            int page  = 0;

            do
            {
                var url = $"{BaseUrl}/{resource}?limit={PageLimit}&start={start}&api_token={Uri.EscapeDataString(accessToken)}";

                var json = await SendWithRetryAsync(url, accessToken);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Pipedrive returns { "success": true/false, "data": [...], ... }
                if (root.TryGetProperty("success", out var success)
                    && success.ValueKind == JsonValueKind.False)
                {
                    var errorMsg = "Unknown error";
                    if (root.TryGetProperty("error", out var errorProp) && errorProp.ValueKind == JsonValueKind.String)
                        errorMsg = errorProp.GetString() ?? errorMsg;

                    throw new ConnectorException(
                        $"Pipedrive API error for '{resource}': {errorMsg}",
                        new InvalidOperationException(errorMsg),
                        "pipedrive");
                }

                // "data" can be null when there are no records.
                if (root.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in data.EnumerateArray())
                    {
                        results.Add(ParseObject(element));
                    }
                }
                else
                {
                    // No data or data is null — stop pagination.
                    break;
                }

                // Pagination: additional_data.pagination.more_items_in_collection + next_start
                start = -1; // sentinel to break unless we find next_start
                if (root.TryGetProperty("additional_data", out var additionalData)
                    && additionalData.TryGetProperty("pagination", out var pagination))
                {
                    var moreItems = false;
                    if (pagination.TryGetProperty("more_items_in_collection", out var more)
                        && more.ValueKind == JsonValueKind.True)
                    {
                        moreItems = true;
                    }

                    if (moreItems
                        && pagination.TryGetProperty("next_start", out var nextStart)
                        && nextStart.ValueKind == JsonValueKind.Number)
                    {
                        start = nextStart.GetInt32();
                    }
                    else
                    {
                        start = -1;
                    }
                }

                page++;
            }
            while (start >= 0 && page < maxPages);

            _logger.LogInformation("Pipedrive: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page + 1);

            return results;
        }

        // ── HTTP with retry / rate-limit handling ────────────────────────────

        private async Task<string> SendWithRetryAsync(string url, string accessToken)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                // Pipedrive uses api_token query param — no auth header needed.
                // But we set Accept to ensure JSON response.
                request.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    // Pipedrive rate limit: 100 requests per 10 seconds.
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds
                                     ?? response.Headers.RetryAfter?.Date?.Subtract(DateTimeOffset.UtcNow).TotalSeconds
                                     ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("Pipedrive rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Pipedrive API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "pipedrive");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("Pipedrive API error {StatusCode} for {Url}: {Body}",
                        (int)response.StatusCode, url, errorBody);

                    throw new ConnectorException(
                        $"Pipedrive API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "pipedrive");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "Pipedrive request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "pipedrive");
        }

        // ── JSON parsing ─────────────────────────────────────────────────────

        private static IDictionary<string, object> ParseObject(JsonElement element)
        {
            IDictionary<string, object> expando = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                expando[prop.Name] = prop.Value.ValueKind switch
                {
                    JsonValueKind.String => (object)(prop.Value.GetString() ?? string.Empty),
                    JsonValueKind.Number => prop.Value.TryGetInt64(out var l) ? l : prop.Value.GetDouble(),
                    JsonValueKind.True   => true,
                    JsonValueKind.False  => false,
                    JsonValueKind.Null   => string.Empty,
                    JsonValueKind.Object => prop.Value.GetRawText(),
                    JsonValueKind.Array  => prop.Value.GetRawText(),
                    _                    => prop.Value.ToString()
                };
            }

            return expando;
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
                    $"Pipedrive connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "pipedrive");
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
