#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
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
    /// Writes data to the Shopify REST Admin API.
    /// POSTs individual records with rate-limit retry and singular resource wrapping.
    /// </summary>
    public class ShopifyWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<ShopifyWriter> _logger;

        private const string DefaultApiVersion = "2024-10";
        private const int MaxRetries = 3;

        /// <summary>
        /// Maps plural resource names to their singular form for Shopify API request bodies.
        /// </summary>
        private static readonly Dictionary<string, string> SingularResourceMap = new(StringComparer.OrdinalIgnoreCase)
        {
            ["orders"]               = "order",
            ["products"]             = "product",
            ["customers"]            = "customer",
            ["inventory_items"]      = "inventory_item",
            ["collections"]          = "collection",
            ["smart_collections"]    = "smart_collection",
            ["custom_collections"]   = "custom_collection",
            ["draft_orders"]         = "draft_order",
            ["abandoned_checkouts"]  = "abandoned_checkout",
            ["locations"]            = "location",
            ["pages"]                = "page",
            ["blogs"]                = "blog",
            ["articles"]             = "article",
            ["metafields"]           = "metafield",
            ["price_rules"]          = "price_rule",
            ["discount_codes"]       = "discount_code",
            ["transactions"]         = "transaction"
        };

        public ShopifyWriter(HttpClient httpClient, ILogger<ShopifyWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── IDestinationWriter ────────────────────────────────────────────────

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var (shop, connStrToken) = ParseConnectionString(config.ConnectionString);
            var resource = GetStringParam(config.Parameters, "resource")
                ?? throw new ConnectorException(
                    "Shopify resource parameter is required.",
                    new ArgumentException("Missing 'resource' parameter."),
                    "shopify");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? connStrToken
                ?? throw new ConnectorException(
                    "Shopify accessToken parameter is required.",
                    new ArgumentException("Missing 'accessToken' parameter."),
                    "shopify");
            var apiVersion = GetStringParam(config.Parameters, "apiVersion") ?? DefaultApiVersion;

            var singularResource = GetSingularResource(resource);
            var url = $"https://{shop}/admin/api/{apiVersion}/{resource}.json";

            _logger.LogInformation(
                "Writing to Shopify resource '{Resource}' at shop '{Shop}' (api={ApiVersion}).",
                resource, shop, apiVersion);

            try
            {
                var count = 0;
                foreach (var record in records)
                {
                    var wrapped = new Dictionary<string, object> { [singularResource] = record };
                    var json = JsonSerializer.Serialize(wrapped);

                    await PostWithRetryAsync(url, accessToken, json);
                    count++;
                }

                _logger.LogInformation(
                    "Successfully wrote {Count} records to Shopify resource '{Resource}'.",
                    count, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to Shopify resource '{Resource}' at '{Shop}'.", resource, shop);
                throw new ConnectorException($"Failed to write to Shopify resource '{resource}': {ex.Message}", ex, "shopify");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Shopify API accepts arbitrary fields — schema validation is not enforced server-side.
            // We could validate against known resource schemas, but for flexibility we return true.
            _logger.LogInformation("Schema validation for Shopify is permissive. Returning true.");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            var resource = GetStringParam(config.Parameters, "resource") ?? "unknown";
            var singularResource = GetSingularResource(resource);

            _logger.LogInformation(
                "Dry run preview for Shopify writer (resource={Resource}, sampleSize={SampleSize}).",
                resource, sampleSize);

            // Show what the actual POST body would look like
            var preview = records.Take(sampleSize).Select(record =>
            {
                IDictionary<string, object> expando = new ExpandoObject();
                expando["_shopify_endpoint"] = $"POST /admin/api/{GetStringParam(config.Parameters, "apiVersion") ?? DefaultApiVersion}/{resource}.json";
                expando["_shopify_body_key"] = singularResource;
                expando["_shopify_payload"] = record;
                return (object)expando;
            });

            return Task.FromResult(preview);
        }

        // ── POST with rate-limit retry ────────────────────────────────────────

        private async Task PostWithRetryAsync(string url, string accessToken, string jsonBody)
        {
            for (var attempt = 0; attempt <= MaxRetries; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, url);
                request.Headers.TryAddWithoutValidation("X-Shopify-Access-Token", accessToken);
                request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

                using var response = await _httpClient.SendAsync(request);

                // Handle rate limiting (HTTP 429)
                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    if (attempt == MaxRetries)
                    {
                        throw new ConnectorException(
                            $"Shopify rate limit exceeded after {MaxRetries} retries.",
                            new HttpRequestException("429 Too Many Requests"),
                            "shopify");
                    }

                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(2);
                    _logger.LogWarning(
                        "Shopify rate limit hit on write. Retrying in {Seconds}s (attempt {Attempt}/{Max}).",
                        retryAfter.TotalSeconds, attempt + 1, MaxRetries);
                    await Task.Delay(retryAfter);
                    continue;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    throw new ConnectorException(
                        $"Shopify API returned {(int)response.StatusCode}: {errorBody}",
                        new HttpRequestException($"HTTP {(int)response.StatusCode}"),
                        "shopify");
                }

                return; // Success
            }
        }

        // ── Singular resource mapping ─────────────────────────────────────────

        private static string GetSingularResource(string pluralResource)
        {
            if (SingularResourceMap.TryGetValue(pluralResource, out var singular))
                return singular;

            // Fallback: strip trailing 's' if present
            return pluralResource.EndsWith("s", StringComparison.OrdinalIgnoreCase)
                ? pluralResource[..^1]
                : pluralResource;
        }

        // ── Parameter helper ──────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static (string shopDomain, string? accessToken) ParseConnectionString(string connectionString)
        {
            try
            {
                using var doc = JsonDocument.Parse(connectionString);
                var root = doc.RootElement;
                var shop = root.TryGetProperty("shopDomain", out var s) ? s.GetString() : null;
                var token = root.TryGetProperty("accessToken", out var t) ? t.GetString() : null;
                return (shop ?? connectionString, token);
            }
            catch (JsonException)
            {
                return (connectionString, null);
            }
        }
    }
}
