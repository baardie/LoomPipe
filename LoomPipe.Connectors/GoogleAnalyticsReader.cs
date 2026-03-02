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
    /// Reads analytics data from Google Analytics 4 using the GA4 Data API (v1beta).
    ///
    /// Parameters:
    ///   accessToken  — OAuth 2.0 access token (or service account token)
    ///   propertyId   — GA4 property ID (e.g. "properties/123456789")
    ///   resource     — report (custom), realtime, metadata
    ///   startDate    — optional start date (e.g. "2024-01-01", "30daysAgo", "yesterday")
    ///   endDate      — optional end date (e.g. "2024-12-31", "today")
    ///   dimensions   — optional comma-separated dimension names
    ///   metrics      — optional comma-separated metric names
    /// </summary>
    public class GoogleAnalyticsReader : ISourceReader
    {
        private const string BaseUrl = "https://analyticsdata.googleapis.com/v1beta";
        private const int DefaultLimit = 10000;
        private const int MaxRetries = 3;

        private static readonly string[] AllResources =
        {
            "report", "realtime", "metadata"
        };

        private static readonly string[] DefaultDimensions =
        {
            "date", "sessionSource", "sessionMedium", "country", "city", "pagePath"
        };

        private static readonly string[] DefaultMetrics =
        {
            "sessions", "totalUsers", "newUsers", "bounceRate", "screenPageViews", "averageSessionDuration"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleAnalyticsReader> _logger;

        public GoogleAnalyticsReader(HttpClient httpClient, ILogger<GoogleAnalyticsReader> logger)
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
            MergeConnectionString(config);

            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetRequiredParam(config.Parameters, "accessToken");
            var propertyId  = GetRequiredParam(config.Parameters, "propertyId");
            var startDate   = GetStringParam(config.Parameters, "startDate");
            var endDate     = GetStringParam(config.Parameters, "endDate");
            var dimensions  = GetStringParam(config.Parameters, "dimensions");
            var metrics     = GetStringParam(config.Parameters, "metrics");

            _logger.LogInformation("GoogleAnalytics: reading resource '{Resource}' for property {PropertyId}.", resource, propertyId);

            try
            {
                return resource.ToLowerInvariant() switch
                {
                    "report"   => await ReadReportAsync(accessToken, propertyId, startDate, endDate, dimensions, metrics, watermarkField, watermarkValue),
                    "realtime" => await ReadRealtimeAsync(accessToken, propertyId, dimensions, metrics),
                    "metadata" => await ReadMetadataAsync(accessToken, propertyId),
                    _ => throw new ConnectorException(
                        $"Unknown Google Analytics resource '{resource}'. Valid resources: {string.Join(", ", AllResources)}.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "googleanalytics")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleAnalytics: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Google Analytics resource '{resource}': {ex.Message}", ex, "googleanalytics");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("GoogleAnalytics: discovering schema for '{Resource}'.", resource);

            try
            {
                // For metadata, fetch the full metadata endpoint
                if (resource.Equals("metadata", StringComparison.OrdinalIgnoreCase))
                {
                    var records = await ReadAsync(config);
                    var first = records.FirstOrDefault() as IDictionary<string, object>;
                    return first?.Keys ?? Array.Empty<string>();
                }

                // For report/realtime, return dimension + metric names
                var dimensions = GetStringParam(config.Parameters, "dimensions");
                var metrics = GetStringParam(config.Parameters, "metrics");

                var dimNames = !string.IsNullOrEmpty(dimensions)
                    ? dimensions.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    : DefaultDimensions;

                var metricNames = !string.IsNullOrEmpty(metrics)
                    ? metrics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    : DefaultMetrics;

                return dimNames.Concat(metricNames);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleAnalytics: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Google Analytics schema for '{resource}': {ex.Message}", ex, "googleanalytics");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            MergeConnectionString(config);

            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("GoogleAnalytics: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleAnalytics: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"GoogleAnalytics dry run preview failed for '{resource}': {ex.Message}", ex, "googleanalytics");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Report reader (with pagination) ─────────────────────────────────

        private async Task<List<object>> ReadReportAsync(
            string accessToken, string propertyId,
            string? startDate, string? endDate,
            string? dimensions, string? metrics,
            string? watermarkField, string? watermarkValue)
        {
            var url = $"{BaseUrl}/{propertyId}:runReport";
            var results = new List<object>();
            int offset = 0;

            // Resolve dimension and metric names
            var dimNames = !string.IsNullOrEmpty(dimensions)
                ? dimensions.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : DefaultDimensions;

            var metricNames = !string.IsNullOrEmpty(metrics)
                ? metrics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : DefaultMetrics;

            // Default date range: last 30 days
            var since = !string.IsNullOrEmpty(startDate) ? startDate : "30daysAgo";
            var until = !string.IsNullOrEmpty(endDate) ? endDate : "today";

            // If watermark is provided, use it as the start date
            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue)
                && watermarkField.Equals("date", StringComparison.OrdinalIgnoreCase))
            {
                since = watermarkValue;
            }

            while (true)
            {
                var requestBody = BuildReportRequestBody(dimNames, metricNames, since, until, DefaultLimit, offset);

                var json = await SendPostWithRetryAsync(url, accessToken, requestBody);
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse rows
                int pageCount = 0;
                if (root.TryGetProperty("rows", out var rows) && rows.ValueKind == JsonValueKind.Array)
                {
                    foreach (var row in rows.EnumerateArray())
                    {
                        results.Add(ParseReportRow(row, dimNames, metricNames));
                        pageCount++;
                    }
                }

                // Check total row count for pagination
                var totalRows = 0;
                if (root.TryGetProperty("rowCount", out var rowCountEl)
                    && rowCountEl.ValueKind == JsonValueKind.Number)
                {
                    totalRows = rowCountEl.GetInt32();
                }

                offset += pageCount;

                if (pageCount < DefaultLimit || offset >= totalRows)
                    break;
            }

            _logger.LogInformation("GoogleAnalytics: read {Count} report rows for property {PropertyId}.",
                results.Count, propertyId);
            return results;
        }

        // ── Realtime reader ─────────────────────────────────────────────────

        private async Task<List<object>> ReadRealtimeAsync(
            string accessToken, string propertyId,
            string? dimensions, string? metrics)
        {
            var url = $"{BaseUrl}/{propertyId}:runRealtimeReport";
            var results = new List<object>();

            var dimNames = !string.IsNullOrEmpty(dimensions)
                ? dimensions.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : new[] { "country", "city" };

            var metricNames = !string.IsNullOrEmpty(metrics)
                ? metrics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : new[] { "activeUsers", "screenPageViews" };

            var requestBody = BuildRealtimeRequestBody(dimNames, metricNames);

            var json = await SendPostWithRetryAsync(url, accessToken, requestBody);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("rows", out var rows) && rows.ValueKind == JsonValueKind.Array)
            {
                foreach (var row in rows.EnumerateArray())
                {
                    results.Add(ParseReportRow(row, dimNames, metricNames));
                }
            }

            _logger.LogInformation("GoogleAnalytics: read {Count} realtime rows for property {PropertyId}.",
                results.Count, propertyId);
            return results;
        }

        // ── Metadata reader ─────────────────────────────────────────────────

        private async Task<List<object>> ReadMetadataAsync(string accessToken, string propertyId)
        {
            var url = $"{BaseUrl}/{propertyId}/metadata";
            var results = new List<object>();

            var json = await SendGetWithRetryAsync(url, accessToken);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Parse dimensions
            if (root.TryGetProperty("dimensions", out var dims) && dims.ValueKind == JsonValueKind.Array)
            {
                foreach (var dim in dims.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["type"] = "dimension";

                    if (dim.TryGetProperty("apiName", out var apiName))
                        row["apiName"] = apiName.GetString() ?? string.Empty;
                    if (dim.TryGetProperty("uiName", out var uiName))
                        row["uiName"] = uiName.GetString() ?? string.Empty;
                    if (dim.TryGetProperty("description", out var desc))
                        row["description"] = desc.GetString() ?? string.Empty;
                    if (dim.TryGetProperty("category", out var cat))
                        row["category"] = cat.GetString() ?? string.Empty;

                    results.Add(row);
                }
            }

            // Parse metrics
            if (root.TryGetProperty("metrics", out var mets) && mets.ValueKind == JsonValueKind.Array)
            {
                foreach (var met in mets.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["type"] = "metric";

                    if (met.TryGetProperty("apiName", out var apiName))
                        row["apiName"] = apiName.GetString() ?? string.Empty;
                    if (met.TryGetProperty("uiName", out var uiName))
                        row["uiName"] = uiName.GetString() ?? string.Empty;
                    if (met.TryGetProperty("description", out var desc))
                        row["description"] = desc.GetString() ?? string.Empty;
                    if (met.TryGetProperty("category", out var cat))
                        row["category"] = cat.GetString() ?? string.Empty;

                    results.Add(row);
                }
            }

            _logger.LogInformation("GoogleAnalytics: read {Count} metadata entries for property {PropertyId}.",
                results.Count, propertyId);
            return results;
        }

        // ── Request body builders ───────────────────────────────────────────

        private static string BuildReportRequestBody(
            string[] dimensions, string[] metrics,
            string startDate, string endDate,
            int limit, int offset)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();

            // dateRanges
            writer.WritePropertyName("dateRanges");
            writer.WriteStartArray();
            writer.WriteStartObject();
            writer.WriteString("startDate", startDate);
            writer.WriteString("endDate", endDate);
            writer.WriteEndObject();
            writer.WriteEndArray();

            // dimensions
            writer.WritePropertyName("dimensions");
            writer.WriteStartArray();
            foreach (var dim in dimensions)
            {
                writer.WriteStartObject();
                writer.WriteString("name", dim);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            // metrics
            writer.WritePropertyName("metrics");
            writer.WriteStartArray();
            foreach (var met in metrics)
            {
                writer.WriteStartObject();
                writer.WriteString("name", met);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteNumber("limit", limit);
            writer.WriteNumber("offset", offset);

            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static string BuildRealtimeRequestBody(string[] dimensions, string[] metrics)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();

            // dimensions
            writer.WritePropertyName("dimensions");
            writer.WriteStartArray();
            foreach (var dim in dimensions)
            {
                writer.WriteStartObject();
                writer.WriteString("name", dim);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            // metrics
            writer.WritePropertyName("metrics");
            writer.WriteStartArray();
            foreach (var met in metrics)
            {
                writer.WriteStartObject();
                writer.WriteString("name", met);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a single GA4 report row into an ExpandoObject.
        /// Row shape: { "dimensionValues": [{"value":"..."},...], "metricValues": [{"value":"..."},...] }
        /// </summary>
        private static IDictionary<string, object> ParseReportRow(
            JsonElement row, string[] dimensionNames, string[] metricNames)
        {
            IDictionary<string, object> expando = new ExpandoObject();

            if (row.TryGetProperty("dimensionValues", out var dimValues)
                && dimValues.ValueKind == JsonValueKind.Array)
            {
                int i = 0;
                foreach (var dv in dimValues.EnumerateArray())
                {
                    var key = i < dimensionNames.Length ? dimensionNames[i] : $"dimension_{i}";
                    expando[key] = dv.TryGetProperty("value", out var val)
                        ? val.GetString() ?? string.Empty
                        : string.Empty;
                    i++;
                }
            }

            if (row.TryGetProperty("metricValues", out var metValues)
                && metValues.ValueKind == JsonValueKind.Array)
            {
                int i = 0;
                foreach (var mv in metValues.EnumerateArray())
                {
                    var key = i < metricNames.Length ? metricNames[i] : $"metric_{i}";
                    var strVal = mv.TryGetProperty("value", out var val)
                        ? val.GetString() ?? "0"
                        : "0";

                    // Try to parse as numeric
                    if (long.TryParse(strVal, out var longVal))
                        expando[key] = longVal;
                    else if (double.TryParse(strVal, System.Globalization.NumberStyles.Float,
                                 System.Globalization.CultureInfo.InvariantCulture, out var dblVal))
                        expando[key] = dblVal;
                    else
                        expando[key] = strVal;

                    i++;
                }
            }

            return expando;
        }

        // ── HTTP helpers with retry / rate-limit handling ────────────────────

        private async Task<string> SendPostWithRetryAsync(string url, string accessToken, string body)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("GoogleAnalytics rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Google Analytics API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "googleanalytics");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("GoogleAnalytics API error {StatusCode}: {Body}",
                        (int)response.StatusCode, errorBody);

                    throw new ConnectorException(
                        $"Google Analytics API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "googleanalytics");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "Google Analytics request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "googleanalytics");
        }

        private async Task<string> SendGetWithRetryAsync(string url, string accessToken)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("GoogleAnalytics rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Google Analytics API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "googleanalytics");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("GoogleAnalytics API error {StatusCode}: {Body}",
                        (int)response.StatusCode, errorBody);

                    throw new ConnectorException(
                        $"Google Analytics API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "googleanalytics");
                }

                return await response.Content.ReadAsStringAsync();
            }

            throw new ConnectorException(
                "Google Analytics request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "googleanalytics");
        }

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        // ── Connection-string merge ─────────────────────────────────────────

        private static void MergeConnectionString(DataSourceConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString))
                return;

            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    return;

                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!config.Parameters.ContainsKey(prop.Name))
                    {
                        config.Parameters[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                            ? prop.Value.GetString()!
                            : prop.Value.GetRawText();
                    }
                }
            }
            catch (JsonException)
            {
                // Not JSON — treat as a plain access token
                if (!config.Parameters.ContainsKey("accessToken"))
                    config.Parameters["accessToken"] = config.ConnectionString;
            }
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
                    $"Google Analytics connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "googleanalytics");
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
