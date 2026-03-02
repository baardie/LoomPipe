#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
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
    /// Reads data from Shopify Plus via the Admin GraphQL API.
    /// Provides access to Plus-specific resources not available in the standard REST Admin API.
    ///
    /// Parameters:
    ///   accessToken — Shopify access token (private app or OAuth)
    ///   shopDomain  — Shopify store domain (e.g. "my-store.myshopify.com")
    ///   resource    — bulk_operations, gift_cards, scripts, multipass, store_credit,
    ///                 b2b_companies, b2b_catalogs, company_locations, price_lists, publications
    ///   query       — optional custom GraphQL query (overrides default per-resource query)
    ///
    /// ConnectionString JSON: {"shopDomain":"...","accessToken":"..."}
    /// </summary>
    public class ShopifyPlusReader : ISourceReader
    {
        private const string DefaultApiVersion = "2024-10";
        private const int DefaultPageSize = 100;

        private static readonly string[] AllResources =
        {
            "bulk_operations", "gift_cards", "scripts", "multipass", "store_credit",
            "b2b_companies", "b2b_catalogs", "company_locations", "price_lists", "publications"
        };

        /// <summary>
        /// Default GraphQL queries per resource. Each query fetches the first page with cursor-based pagination.
        /// The {FIRST} and {AFTER} tokens are replaced at runtime.
        /// </summary>
        private static readonly Dictionary<string, string> DefaultQueries = new(StringComparer.OrdinalIgnoreCase)
        {
            ["bulk_operations"] = @"{ currentBulkOperation { id status url } }",
            ["gift_cards"] = @"{ giftCards(first:{FIRST}{AFTER}) { nodes { id balance { amount currencyCode } createdAt disabledAt expiresOn lastCharacters note customer { id } } pageInfo { hasNextPage endCursor } } }",
            ["scripts"] = @"{ scriptTags(first:{FIRST}{AFTER}) { nodes { id src displayScope createdAt updatedAt } pageInfo { hasNextPage endCursor } } }",
            ["multipass"] = @"{ shop { id name multipassEnabled: features { multipass { eligibleForMultipass } } } }",
            ["store_credit"] = @"{ customers(first:{FIRST}{AFTER}) { nodes { id displayName email storeCreditAccounts(first:5) { nodes { id balance { amount currencyCode } } } } pageInfo { hasNextPage endCursor } } }",
            ["b2b_companies"] = @"{ companies(first:{FIRST}{AFTER}) { nodes { id name note externalId createdAt updatedAt contactCount locationCount } pageInfo { hasNextPage endCursor } } }",
            ["b2b_catalogs"] = @"{ catalogs(first:{FIRST}{AFTER}) { nodes { id title status } pageInfo { hasNextPage endCursor } } }",
            ["company_locations"] = @"{ companyLocations(first:{FIRST}{AFTER}) { nodes { id name externalId createdAt updatedAt company { id name } billingAddress { address1 city countryCode } } pageInfo { hasNextPage endCursor } } }",
            ["price_lists"] = @"{ priceLists(first:{FIRST}{AFTER}) { nodes { id name currency fixedPricesCount } pageInfo { hasNextPage endCursor } } }",
            ["publications"] = @"{ publications(first:{FIRST}{AFTER}) { nodes { id name supportsFuturePublishing } pageInfo { hasNextPage endCursor } } }"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ShopifyPlusReader> _logger;

        public ShopifyPlusReader(HttpClient httpClient, ILogger<ShopifyPlusReader> logger)
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
            var parameters  = MergeConnectionString(config);
            var shopDomain  = GetRequiredParam(parameters, "shopDomain");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var customQuery = GetStringParam(parameters, "query");

            _logger.LogInformation("ShopifyPlus: reading resource '{Resource}' from '{Shop}'.", resource, shopDomain);

            try
            {
                return await ReadFullAsync(shopDomain, accessToken, resource, customQuery);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ShopifyPlus: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read ShopifyPlus resource '{resource}': {ex.Message}", ex, "shopifyplus");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var shopDomain  = GetRequiredParam(parameters, "shopDomain");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var customQuery = GetStringParam(parameters, "query");

            _logger.LogInformation("ShopifyPlus: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(shopDomain, accessToken, resource, customQuery, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ShopifyPlus: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover ShopifyPlus schema for '{resource}': {ex.Message}", ex, "shopifyplus");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var shopDomain  = GetRequiredParam(parameters, "shopDomain");
            var accessToken = GetAccessToken(parameters, config);
            var resource    = GetRequiredParam(parameters, "resource");
            var customQuery = GetStringParam(parameters, "query");

            _logger.LogInformation("ShopifyPlus: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(shopDomain, accessToken, resource, customQuery, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ShopifyPlus: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"ShopifyPlus dry run preview failed for '{resource}': {ex.Message}", ex, "shopifyplus");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based GraphQL pagination) ──────────────────────

        private async Task<List<object>> ReadFullAsync(
            string shopDomain, string accessToken, string resource, string? customQuery,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? cursor = null;
            bool hasNextPage = true;
            int page = 0;

            var graphqlUrl = $"https://{shopDomain}/admin/api/{DefaultApiVersion}/graphql.json";

            while (hasNextPage && page < maxPages)
            {
                var query = BuildQuery(resource, customQuery, cursor);
                var requestBody = JsonSerializer.Serialize(new { query });

                using var request = new HttpRequestMessage(HttpMethod.Post, graphqlUrl)
                {
                    Content = new StringContent(requestBody, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Check for GraphQL errors
                if (doc.RootElement.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array)
                {
                    var firstError = errors.EnumerateArray().FirstOrDefault();
                    var errorMsg = firstError.TryGetProperty("message", out var msg)
                        ? msg.GetString() ?? "Unknown GraphQL error"
                        : "Unknown GraphQL error";
                    throw new ConnectorException(
                        $"ShopifyPlus GraphQL error: {errorMsg}",
                        new InvalidOperationException(errorMsg),
                        "shopifyplus");
                }

                if (!doc.RootElement.TryGetProperty("data", out var data))
                    break;

                var (items, nextCursor, hasMore) = ParseGraphQLResponse(data, resource);
                results.AddRange(items);

                cursor = nextCursor;
                hasNextPage = hasMore;
                page++;
            }

            _logger.LogInformation("ShopifyPlus: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Query builder ────────────────────────────────────────────────────

        private static string BuildQuery(string resource, string? customQuery, string? cursor)
        {
            if (!string.IsNullOrWhiteSpace(customQuery))
            {
                // Replace pagination tokens in custom queries
                var q = customQuery
                    .Replace("{FIRST}", DefaultPageSize.ToString())
                    .Replace("{AFTER}", cursor != null ? $", after:\"{cursor}\"" : "");
                return q;
            }

            if (!DefaultQueries.TryGetValue(resource, out var template))
            {
                throw new ConnectorException(
                    $"ShopifyPlus: no default query for resource '{resource}'. Provide a custom 'query' parameter.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "shopifyplus");
            }

            return template
                .Replace("{FIRST}", DefaultPageSize.ToString())
                .Replace("{AFTER}", cursor != null ? $", after:\"{cursor}\"" : "");
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses the GraphQL data object, finding the first connection-style field
        /// with nodes and pageInfo.
        /// </summary>
        private static (List<object> Items, string? Cursor, bool HasNextPage) ParseGraphQLResponse(
            JsonElement data, string resource)
        {
            var items = new List<object>();
            string? cursor = null;
            bool hasNextPage = false;

            // Walk the data object to find the first field that has "nodes" and "pageInfo"
            JsonElement target = data;
            foreach (var prop in data.EnumerateObject())
            {
                target = prop.Value;
                break;
            }

            // Handle nested connections (e.g. data.giftCards.nodes)
            if (target.TryGetProperty("nodes", out var nodes) && nodes.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in nodes.EnumerateArray())
                {
                    items.Add(FlattenElement(element));
                }

                if (target.TryGetProperty("pageInfo", out var pageInfo))
                {
                    if (pageInfo.TryGetProperty("hasNextPage", out var hnp))
                        hasNextPage = hnp.GetBoolean();
                    if (pageInfo.TryGetProperty("endCursor", out var ec) && ec.ValueKind == JsonValueKind.String)
                        cursor = ec.GetString();
                }
            }
            else
            {
                // Non-paginated response (e.g. currentBulkOperation, shop)
                items.Add(FlattenElement(target));
                hasNextPage = false;
            }

            return (items, cursor, hasNextPage);
        }

        private static object FlattenElement(JsonElement element)
        {
            if (element.ValueKind != JsonValueKind.Object)
                return ConvertJsonValue(element);

            IDictionary<string, object> row = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                if (prop.Value.ValueKind == JsonValueKind.Object)
                {
                    // Flatten nested objects (e.g. balance { amount, currencyCode } -> balance_amount, balance_currencyCode)
                    foreach (var nested in prop.Value.EnumerateObject())
                    {
                        row[$"{prop.Name}_{nested.Name}"] = ConvertJsonValue(nested.Value);
                    }
                }
                else
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }
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
            request.Headers.TryAddWithoutValidation("X-Shopify-Access-Token", accessToken);
        }

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(token))
                return token;

            throw new ConnectorException(
                "ShopifyPlus access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                new ArgumentException("Missing 'accessToken'."),
                "shopifyplus");
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"shopDomain":"...","accessToken":"..."}
        /// Parameters take precedence; connection string provides defaults.
        /// </summary>
        private static Dictionary<string, object> MergeConnectionString(DataSourceConfig config)
        {
            var merged = new Dictionary<string, object>(config.Parameters ?? new Dictionary<string, object>());
            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString ?? "{}");
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    if (!merged.ContainsKey(prop.Name) || string.IsNullOrWhiteSpace(GetStringParam(merged, prop.Name)))
                        merged[prop.Name] = prop.Value.Clone();
                }
            }
            catch (JsonException) { /* connection string is not JSON — ignore */ }

            return merged;
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
                    $"ShopifyPlus connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "shopifyplus");
            return value;
        }
    }
}
