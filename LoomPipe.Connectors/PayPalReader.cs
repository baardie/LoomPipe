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
    /// Reads data from the PayPal REST API v1/v2.
    ///
    /// Parameters:
    ///   clientId      — PayPal app client ID
    ///   clientSecret  — PayPal app client secret
    ///   resource      — transactions, payments, orders, invoices, subscriptions,
    ///                   disputes, products, plans, payouts
    ///   startDate     — optional ISO date for transaction queries
    ///   endDate       — optional ISO date for transaction queries
    ///   useSandbox    — "true" to use sandbox API (default: production)
    ///
    /// Alternatively, provide a JSON connection string: {"clientId":"...","clientSecret":"..."}
    /// </summary>
    public class PayPalReader : ISourceReader
    {
        private const string ProductionUrl = "https://api-m.paypal.com";
        private const string SandboxUrl = "https://api-m.sandbox.paypal.com";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "transactions", "payments", "orders", "invoices", "subscriptions",
            "disputes", "products", "plans", "payouts"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<PayPalReader> _logger;

        public PayPalReader(HttpClient httpClient, ILogger<PayPalReader> logger)
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
            var (clientId, clientSecret) = ResolveCredentials(config);
            var baseUrl = GetBaseUrl(config.Parameters);

            _logger.LogInformation("PayPal: reading resource '{Resource}'.", resource);

            try
            {
                var bearerToken = await AcquireBearerTokenAsync(clientId, clientSecret, baseUrl);
                return await ReadFullAsync(resource, bearerToken, baseUrl, config.Parameters);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PayPal: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read PayPal resource '{resource}': {ex.Message}", ex, "paypal");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (clientId, clientSecret) = ResolveCredentials(config);
            var baseUrl = GetBaseUrl(config.Parameters);

            _logger.LogInformation("PayPal: discovering schema for '{Resource}'.", resource);

            try
            {
                var bearerToken = await AcquireBearerTokenAsync(clientId, clientSecret, baseUrl);
                var sample = await ReadFullAsync(resource, bearerToken, baseUrl, config.Parameters, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PayPal: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover PayPal schema for '{resource}': {ex.Message}", ex, "paypal");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (clientId, clientSecret) = ResolveCredentials(config);
            var baseUrl = GetBaseUrl(config.Parameters);

            _logger.LogInformation("PayPal: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var bearerToken = await AcquireBearerTokenAsync(clientId, clientSecret, baseUrl);
                var records = await ReadFullAsync(resource, bearerToken, baseUrl, config.Parameters, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PayPal: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"PayPal dry run preview failed for '{resource}': {ex.Message}", ex, "paypal");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── OAuth2 token acquisition ─────────────────────────────────────────

        private async Task<string> AcquireBearerTokenAsync(string clientId, string clientSecret, string baseUrl)
        {
            var url = $"{baseUrl}/v1/oauth2/token";

            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("grant_type", "client_credentials")
                })
            };

            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{clientId}:{clientSecret}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("access_token", out var tokenEl)
                && tokenEl.ValueKind == JsonValueKind.String)
            {
                return tokenEl.GetString()!;
            }

            throw new ConnectorException(
                "PayPal: failed to acquire Bearer token — 'access_token' not found in response.",
                new InvalidOperationException("Missing access_token in OAuth2 response."),
                "paypal");
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string bearerToken, string baseUrl,
            Dictionary<string, object> parameters, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageNum = 1;
            int page = 0;
            bool hasMore = true;

            do
            {
                var url = BuildUrl(resource, baseUrl, parameters, pageNum);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, bearerToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                hasMore = ParseResponse(doc.RootElement, resource, results, out int count);

                pageNum++;
                page++;
            }
            while (hasMore && page < maxPages);

            _logger.LogInformation("PayPal: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string baseUrl, Dictionary<string, object> parameters, int pageNum)
        {
            var startDate = GetStringParam(parameters, "startDate")
                ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-ddTHH:mm:ssZ");
            var endDate = GetStringParam(parameters, "endDate")
                ?? DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

            return resource switch
            {
                "transactions" => $"{baseUrl}/v1/reporting/transactions?start_date={Uri.EscapeDataString(startDate)}&end_date={Uri.EscapeDataString(endDate)}&page_size={PageSize}&page={pageNum}",
                "payments"     => $"{baseUrl}/v1/payments/payment?count={PageSize}&start_index={((pageNum - 1) * PageSize)}",
                "orders"       => $"{baseUrl}/v2/checkout/orders?page_size={PageSize}&page={pageNum}",
                "invoices"     => $"{baseUrl}/v2/invoicing/invoices?page_size={PageSize}&page={pageNum}",
                "subscriptions"=> $"{baseUrl}/v1/billing/subscriptions?page_size={PageSize}&page={pageNum}",
                "disputes"     => $"{baseUrl}/v1/customer/disputes?page_size={PageSize}&start_time={Uri.EscapeDataString(startDate)}",
                "products"     => $"{baseUrl}/v1/catalogs/products?page_size={PageSize}&page={pageNum}",
                "plans"        => $"{baseUrl}/v1/billing/plans?page_size={PageSize}&page={pageNum}",
                "payouts"      => $"{baseUrl}/v1/payments/payouts?page_size={PageSize}&page={pageNum}",
                _ => throw new ConnectorException(
                    $"PayPal: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "paypal")
            };
        }

        private static string GetBaseUrl(Dictionary<string, object> parameters)
        {
            var useSandbox = GetStringParam(parameters, "useSandbox");
            return string.Equals(useSandbox, "true", StringComparison.OrdinalIgnoreCase)
                ? SandboxUrl
                : ProductionUrl;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static bool ParseResponse(JsonElement root, string resource, List<object> results, out int count)
        {
            count = 0;
            JsonElement items;
            bool hasMore = false;

            // transactions: { "transaction_details": [...], "total_pages": N, "page": N }
            if (resource == "transactions"
                && root.TryGetProperty("transaction_details", out items)
                && items.ValueKind == JsonValueKind.Array)
            {
                count = FlattenArray(items, results);

                if (root.TryGetProperty("total_pages", out var totalPages)
                    && root.TryGetProperty("page", out var currentPage))
                {
                    hasMore = currentPage.GetInt32() < totalPages.GetInt32();
                }

                return hasMore;
            }

            // payments: { "payments": [...], "count": N }
            if (root.TryGetProperty("payments", out items) && items.ValueKind == JsonValueKind.Array)
            {
                count = FlattenArray(items, results);
                hasMore = count >= PageSize;
                return hasMore;
            }

            // Generic: try "items", "plans", "products", "links", etc.
            foreach (var key in new[] { "items", "plans", "products", "disputes", "invoices" })
            {
                if (root.TryGetProperty(key, out items) && items.ValueKind == JsonValueKind.Array)
                {
                    count = FlattenArray(items, results);
                    hasMore = count >= PageSize;
                    return hasMore;
                }
            }

            // Array root
            if (root.ValueKind == JsonValueKind.Array)
            {
                count = FlattenArray(root, results);
                hasMore = count >= PageSize;
                return hasMore;
            }

            // Check for "links" with rel=next for HATEOAS pagination
            if (root.TryGetProperty("links", out var links) && links.ValueKind == JsonValueKind.Array)
            {
                foreach (var link in links.EnumerateArray())
                {
                    if (link.TryGetProperty("rel", out var rel)
                        && rel.GetString() == "next")
                    {
                        hasMore = true;
                        break;
                    }
                }
            }

            return hasMore;
        }

        private static int FlattenArray(JsonElement items, List<object> results)
        {
            int count = 0;

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten one level of nested objects (e.g. "transaction_info", "payer_info").
                        foreach (var nested in prop.Value.EnumerateObject())
                        {
                            if (nested.Value.ValueKind == JsonValueKind.Object || nested.Value.ValueKind == JsonValueKind.Array)
                                row[$"{prop.Name}_{nested.Name}"] = nested.Value.ToString();
                            else
                                row[$"{prop.Name}_{nested.Name}"] = ConvertJsonValue(nested.Value);
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        row[prop.Name] = prop.Value.ToString();
                    }
                    else
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
                count++;
            }

            return count;
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

        private static void ApplyAuth(HttpRequestMessage request, string bearerToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
        }

        // ── Credential resolution ────────────────────────────────────────────

        private (string ClientId, string ClientSecret) ResolveCredentials(DataSourceConfig config)
        {
            var clientId     = GetStringParam(config.Parameters, "clientId");
            var clientSecret = GetStringParam(config.Parameters, "clientSecret");

            if (!string.IsNullOrWhiteSpace(clientId) && !string.IsNullOrWhiteSpace(clientSecret))
                return (clientId, clientSecret);

            // Fall back to connection string JSON: {"clientId":"...","clientSecret":"..."}
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    var root = doc.RootElement;

                    var id     = root.TryGetProperty("clientId", out var cid) ? cid.GetString() : null;
                    var secret = root.TryGetProperty("clientSecret", out var cs) ? cs.GetString() : null;

                    if (!string.IsNullOrWhiteSpace(id) && !string.IsNullOrWhiteSpace(secret))
                        return (id!, secret!);
                }
                catch (JsonException)
                {
                    // Not valid JSON — fall through to error.
                }
            }

            throw new ConnectorException(
                "PayPal credentials are required. Provide 'clientId' and 'clientSecret' via Parameters or a JSON connection string.",
                new ArgumentException("Missing PayPal credentials."),
                "paypal");
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
                    $"PayPal connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "paypal");
            return value;
        }
    }
}
