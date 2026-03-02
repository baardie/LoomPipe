#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    public class StripeReader : ISourceReader
    {
        private const string BaseUrl = "https://api.stripe.com/v1";
        private const int DefaultLimit = 100;
        private const int MaxRetries = 3;

        private readonly HttpClient _httpClient;
        private readonly ILogger<StripeReader> _logger;

        /// <summary>
        /// Maps logical resource names to their Stripe API path segments.
        /// Most are identity mappings; exceptions are listed explicitly.
        /// </summary>
        private static readonly Dictionary<string, string> ResourcePathMap = new(StringComparer.OrdinalIgnoreCase)
        {
            ["charges"]              = "charges",
            ["customers"]            = "customers",
            ["subscriptions"]        = "subscriptions",
            ["invoices"]             = "invoices",
            ["invoice_items"]        = "invoiceitems",
            ["payment_intents"]      = "payment_intents",
            ["products"]             = "products",
            ["prices"]               = "prices",
            ["refunds"]              = "refunds",
            ["disputes"]             = "disputes",
            ["payouts"]              = "payouts",
            ["balance_transactions"] = "balance_transactions",
            ["events"]               = "events",
            ["checkout_sessions"]    = "checkout/sessions",
            ["coupons"]              = "coupons",
            ["credit_notes"]         = "credit_notes",
            ["promotion_codes"]      = "promotion_codes",
        };

        public StripeReader(HttpClient httpClient, ILogger<StripeReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ─────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var apiKey = GetStringParam(config.Parameters, "apiKey")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Stripe API key is required. Provide it via Parameters['apiKey'] or the connection string.",
                    new ArgumentException("Missing 'apiKey'."),
                    "stripe");
            var limit    = GetLimit(config.Parameters);

            _logger.LogInformation("Stripe ReadAsync: resource={Resource}, limit={Limit}, watermark={Watermark}.",
                resource, limit, watermarkValue ?? "(none)");

            try
            {
                var allRecords = new List<object>();
                string? startingAfter = null;

                while (true)
                {
                    var url = BuildUrl(resource, limit, watermarkValue, startingAfter,
                        GetStringParam(config.Parameters, "startDate"));

                    var json = await SendWithRetryAsync(url, apiKey);
                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;

                    if (!root.TryGetProperty("data", out var dataArray) ||
                        dataArray.ValueKind != JsonValueKind.Array)
                    {
                        throw new ConnectorException(
                            $"Stripe API response for '{resource}' did not contain a 'data' array.",
                            new InvalidOperationException("Missing or invalid 'data' property in Stripe response."),
                            "stripe");
                    }

                    string? lastId = null;
                    foreach (var element in dataArray.EnumerateArray())
                    {
                        var row = ParseStripeObject(element);
                        allRecords.Add(row);

                        if (element.TryGetProperty("id", out var idProp))
                            lastId = idProp.GetString();
                    }

                    // Check for more pages
                    var hasMore = root.TryGetProperty("has_more", out var hasMoreProp)
                                  && hasMoreProp.ValueKind == JsonValueKind.True;

                    if (!hasMore || lastId == null)
                        break;

                    startingAfter = lastId;
                }

                _logger.LogInformation("Stripe ReadAsync completed: {Count} records from '{Resource}'.",
                    allRecords.Count, resource);
                return allRecords;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Stripe resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Stripe resource '{resource}': {ex.Message}", ex, "stripe");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("Discovering schema for Stripe resource '{Resource}'.", resource);

            try
            {
                // Fetch a small sample to infer schema
                var originalLimit = GetStringParam(config.Parameters, "limit");
                config.Parameters["limit"] = "5";

                var records = await ReadAsync(config);

                // Restore original limit
                if (originalLimit != null)
                    config.Parameters["limit"] = originalLimit;
                else
                    config.Parameters.Remove("limit");

                var first = records.FirstOrDefault();
                if (first is IDictionary<string, object> dict)
                    return dict.Keys;

                return Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Stripe resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Stripe schema for '{resource}': {ex.Message}", ex, "stripe");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            _logger.LogInformation("Dry run preview for Stripe resource '{Resource}' (sampleSize={SampleSize}).",
                resource, sampleSize);

            // Override limit to only fetch what we need
            var originalLimit = GetStringParam(config.Parameters, "limit");
            config.Parameters["limit"] = Math.Min(sampleSize, DefaultLimit).ToString();

            var records = await ReadAsync(config);

            // Restore original limit
            if (originalLimit != null)
                config.Parameters["limit"] = originalLimit;
            else
                config.Parameters.Remove("limit");

            return records.Take(sampleSize);
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            IEnumerable<string> resources = new[]
            {
                "charges",
                "customers",
                "subscriptions",
                "invoices",
                "invoice_items",
                "payment_intents",
                "products",
                "prices",
                "refunds",
                "disputes",
                "payouts",
                "balance_transactions",
                "events",
                "checkout_sessions",
                "coupons",
                "credit_notes",
                "promotion_codes"
            };
            return Task.FromResult(resources);
        }

        // ── URL builder ───────────────────────────────────────────────────────

        private static string BuildUrl(
            string resource,
            int limit,
            string? watermarkValue,
            string? startingAfter,
            string? startDate)
        {
            var apiPath = ResolveResourcePath(resource);
            var url = $"{BaseUrl}/{apiPath}?limit={limit}";

            // Watermark-based incremental sync (takes precedence over startDate)
            if (!string.IsNullOrEmpty(watermarkValue))
            {
                var unixTs = ConvertToUnixTimestamp(watermarkValue);
                if (unixTs.HasValue)
                    url += $"&created%5Bgte%5D={unixTs.Value}";
            }
            else if (!string.IsNullOrEmpty(startDate))
            {
                var unixTs = ConvertToUnixTimestamp(startDate);
                if (unixTs.HasValue)
                    url += $"&created%5Bgte%5D={unixTs.Value}";
            }

            if (!string.IsNullOrEmpty(startingAfter))
                url += $"&starting_after={startingAfter}";

            return url;
        }

        private static string ResolveResourcePath(string resource)
        {
            if (ResourcePathMap.TryGetValue(resource, out var path))
                return path;

            // Fall back to using the resource name directly
            return resource;
        }

        private static long? ConvertToUnixTimestamp(string dateString)
        {
            // Try parsing as ISO 8601 date/datetime
            if (DateTimeOffset.TryParse(dateString, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal, out var dto))
            {
                return dto.ToUnixTimeSeconds();
            }

            // Try parsing as a raw unix timestamp
            if (long.TryParse(dateString, out var rawUnix))
                return rawUnix;

            return null;
        }

        private static int GetLimit(Dictionary<string, object> parameters)
        {
            var limitStr = GetStringParam(parameters, "limit");
            if (!string.IsNullOrEmpty(limitStr) && int.TryParse(limitStr, out var parsed))
                return Math.Clamp(parsed, 1, 100);
            return DefaultLimit;
        }

        // ── HTTP with retry / rate-limit handling ────────────────────────────

        private async Task<string> SendWithRetryAsync(string url, string apiKey)
        {
            for (int attempt = 1; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                using var response = await _httpClient.SendAsync(request);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta?.TotalSeconds
                                     ?? response.Headers.RetryAfter?.Date?.Subtract(DateTimeOffset.UtcNow).TotalSeconds
                                     ?? 2.0;
                    var delayMs = (int)(Math.Max(retryAfter, 1.0) * 1000);

                    _logger.LogWarning("Stripe rate limited (429). Retry {Attempt}/{MaxRetries} after {DelayMs}ms.",
                        attempt, MaxRetries, delayMs);

                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Stripe API rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException($"HTTP 429 Too Many Requests for {url}"),
                            "stripe");
                    }

                    await Task.Delay(delayMs);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("Stripe API error {StatusCode} for {Url}: {Body}",
                        (int)response.StatusCode, url, errorBody);

                    throw new ConnectorException(
                        $"Stripe API returned {(int)response.StatusCode}: {TruncateErrorBody(errorBody)}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode} for {url}"),
                        "stripe");
                }

                return await response.Content.ReadAsStringAsync();
            }

            // Should not reach here, but satisfy the compiler
            throw new ConnectorException(
                "Stripe request failed after all retries.",
                new HttpRequestException("Max retries exceeded."),
                "stripe");
        }

        // ── JSON parsing ─────────────────────────────────────────────────────

        /// <summary>
        /// Parses a single Stripe JSON object into an ExpandoObject.
        /// Top-level primitives are mapped directly; nested objects/arrays are
        /// JSON-serialized as string values.
        /// </summary>
        private static IDictionary<string, object> ParseStripeObject(JsonElement element)
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
                    // Nested objects and arrays are serialized as JSON strings
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
            {
                throw new ConnectorException(
                    $"Required Stripe parameter '{key}' is missing or empty.",
                    new ArgumentException($"Parameter '{key}' is required.", key),
                    "stripe");
            }
            return value;
        }

        private static string TruncateErrorBody(string body, int maxLength = 500)
            => body.Length <= maxLength ? body : body[..maxLength] + "...";
    }
}
