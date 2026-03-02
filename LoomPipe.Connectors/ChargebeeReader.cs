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
    /// Reads records from Chargebee using the v2 API.
    ///
    /// Parameters:
    ///   accessToken  — Chargebee API key (used as Basic auth username, empty password)
    ///   site         — Chargebee site/subdomain (e.g. "mycompany" for mycompany.chargebee.com)
    ///   resource     — customers, subscriptions, invoices, plans, addons, coupons, credit_notes,
    ///                  transactions, events, items, item_prices, quotes, orders, payment_sources, gifts
    /// </summary>
    public class ChargebeeReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "customers", "subscriptions", "invoices", "plans", "addons",
            "coupons", "credit_notes", "transactions", "events", "items",
            "item_prices", "quotes", "orders", "payment_sources", "gifts"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ChargebeeReader> _logger;

        public ChargebeeReader(HttpClient httpClient, ILogger<ChargebeeReader> logger)
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
            var resource = GetRequiredParam(config.Parameters, "resource");
            var site     = GetRequiredParam(config.Parameters, "site");
            var apiKey   = ResolveApiKey(config);

            _logger.LogInformation("Chargebee: reading resource '{Resource}' from site '{Site}'.", resource, site);

            try
            {
                return await ReadPaginatedAsync(site, apiKey, resource, watermarkField, watermarkValue);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Chargebee: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Chargebee resource '{resource}': {ex.Message}", ex, "chargebee");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var site     = GetRequiredParam(config.Parameters, "site");
            var apiKey   = ResolveApiKey(config);

            _logger.LogInformation("Chargebee: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadPaginatedAsync(site, apiKey, resource, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Chargebee: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Chargebee schema for '{resource}': {ex.Message}", ex, "chargebee");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var site     = GetRequiredParam(config.Parameters, "site");
            var apiKey   = ResolveApiKey(config);

            _logger.LogInformation("Chargebee: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(site, apiKey, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Chargebee: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Chargebee dry run preview failed for '{resource}': {ex.Message}", ex, "chargebee");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string site, string apiKey, string resource,
            string? watermarkField = null, string? watermarkValue = null,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextOffset = null;
            int page = 0;

            // Derive the singular entity key from the resource name (e.g. "customers" -> "customer").
            var entityKey = GetEntityKey(resource);

            do
            {
                var url = BuildUrl(site, resource, nextOffset, watermarkField, watermarkValue);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, apiKey);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, entityKey, results);

                // Chargebee pagination: "next_offset" in root.
                nextOffset = null;
                if (doc.RootElement.TryGetProperty("next_offset", out var nextOffsetEl)
                    && nextOffsetEl.ValueKind == JsonValueKind.String)
                {
                    nextOffset = nextOffsetEl.GetString();
                }

                page++;
            }
            while (nextOffset != null && page < maxPages);

            _logger.LogInformation("Chargebee: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string site, string resource, string? nextOffset,
            string? watermarkField = null, string? watermarkValue = null)
        {
            var sb = new StringBuilder($"https://{site}.chargebee.com/api/v2/{resource}");
            sb.Append($"?limit={PageLimit}");

            if (!string.IsNullOrEmpty(nextOffset))
            {
                sb.Append($"&offset={Uri.EscapeDataString(nextOffset)}");
            }

            // Chargebee uses sort_by and filter params for incremental loads.
            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
            {
                sb.Append($"&{Uri.EscapeDataString($"{watermarkField}[after]")}={Uri.EscapeDataString(watermarkValue)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Chargebee responses have shape: { "list": [{ "customer": {...} }, ...], "next_offset": "..." }
        /// Each element in "list" wraps the entity under a key matching the singular resource name.
        /// </summary>
        private static void ParseResultsPage(JsonElement root, string entityKey, List<object> results)
        {
            if (!root.TryGetProperty("list", out var list) || list.ValueKind != JsonValueKind.Array)
            {
                // Fallback: if root is an array directly.
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in root.EnumerateArray())
                    {
                        results.Add(FlattenElement(element));
                    }
                }
                return;
            }

            foreach (var wrapper in list.EnumerateArray())
            {
                // Try to extract the entity from the wrapper (e.g. wrapper["customer"]).
                if (wrapper.TryGetProperty(entityKey, out var entity) && entity.ValueKind == JsonValueKind.Object)
                {
                    results.Add(FlattenElement(entity));
                }
                else
                {
                    // Flatten the entire wrapper if entity key not found.
                    results.Add(FlattenElement(wrapper));
                }
            }
        }

        private static object FlattenElement(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in element.EnumerateObject())
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

        // ── Entity key helper ────────────────────────────────────────────────

        /// <summary>
        /// Derives the singular entity key from the plural resource name.
        /// Chargebee wraps each list item under the singular form of the resource.
        /// </summary>
        private static string GetEntityKey(string resource)
        {
            // Known mappings where simple "s" removal doesn't work.
            return resource switch
            {
                "customers"       => "customer",
                "subscriptions"   => "subscription",
                "invoices"        => "invoice",
                "plans"           => "plan",
                "addons"          => "addon",
                "coupons"         => "coupon",
                "credit_notes"    => "credit_note",
                "transactions"    => "transaction",
                "events"          => "event",
                "items"           => "item",
                "item_prices"     => "item_price",
                "quotes"          => "quote",
                "orders"          => "order",
                "payment_sources" => "payment_source",
                "gifts"           => "gift",
                _                 => resource.TrimEnd('s')
            };
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string apiKey)
        {
            // Chargebee uses Basic auth: API key as username, empty password.
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", encoded);
        }

        private string ResolveApiKey(DataSourceConfig config)
        {
            return GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Chargebee API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "chargebee");
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
                    $"Chargebee connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "chargebee");
            return value;
        }
    }
}
