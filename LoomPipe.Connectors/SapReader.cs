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
    /// Reads data from SAP S/4HANA or SAP Business One via the OData API.
    ///
    /// Parameters:
    ///   accessToken — Bearer token (optional; used when OAuth is configured)
    ///   username    — SAP basic auth username (alternative to accessToken)
    ///   password    — SAP basic auth password (alternative to accessToken)
    ///   host        — SAP system hostname (e.g. "my-s4hana.example.com")
    ///   resource    — OData entity set name (e.g. "BusinessPartner", "SalesOrder")
    ///   sapClient   — optional SAP client number (e.g. "100")
    ///
    /// ConnectionString JSON: {"host":"...","username":"...","password":"...","sapClient":"100"}
    /// </summary>
    public class SapReader : ISourceReader
    {
        private const string BasePath = "/sap/opu/odata/sap/";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "BusinessPartner", "SalesOrder", "PurchaseOrder", "Material",
            "GLAccount", "CostCenter", "ProfitCenter",
            "ProductionOrder", "MaintenanceOrder", "Employee"
        };

        /// <summary>
        /// Maps resource names to their OData service + entity set paths.
        /// Pattern: API_{SERVICE_NAME}/A_{EntitySet}
        /// </summary>
        private static readonly Dictionary<string, string> ResourceEndpoints = new(StringComparer.OrdinalIgnoreCase)
        {
            ["BusinessPartner"]  = "API_BUSINESS_PARTNER/A_BusinessPartner",
            ["SalesOrder"]       = "API_SALES_ORDER_SRV/A_SalesOrder",
            ["PurchaseOrder"]    = "API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder",
            ["Material"]         = "API_PRODUCT_SRV/A_Product",
            ["GLAccount"]        = "API_GLACCOUNTINCHARTOFACCOUNTS_SRV/A_GLAccountInChartOfAccounts",
            ["CostCenter"]       = "API_COSTCENTER_SRV/A_CostCenter",
            ["ProfitCenter"]     = "API_PROFITCENTER_SRV/A_ProfitCenter",
            ["ProductionOrder"]  = "API_PRODUCTION_ORDERS/A_ProductionOrder_2",
            ["MaintenanceOrder"] = "API_MAINTENANCEORDER/MaintenanceOrder",
            ["Employee"]         = "API_BUSINESS_PARTNER/A_BusinessPartner"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SapReader> _logger;

        public SapReader(HttpClient httpClient, ILogger<SapReader> logger)
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
            var host        = GetRequiredParam(parameters, "host");
            var resource    = GetRequiredParam(parameters, "resource");
            var sapClient   = GetStringParam(parameters, "sapClient");

            _logger.LogInformation("SAP: reading resource '{Resource}' from host '{Host}'.", resource, host);

            try
            {
                string? filter = null;
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    filter = $"{watermarkField} ge '{watermarkValue}'";
                }

                return await ReadFullAsync(host, parameters, config, resource, sapClient, filter);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SAP: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read SAP resource '{resource}': {ex.Message}", ex, "sap");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var host        = GetRequiredParam(parameters, "host");
            var resource    = GetRequiredParam(parameters, "resource");
            var sapClient   = GetStringParam(parameters, "sapClient");

            _logger.LogInformation("SAP: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(host, parameters, config, resource, sapClient, filter: null, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SAP: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover SAP schema for '{resource}': {ex.Message}", ex, "sap");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var host        = GetRequiredParam(parameters, "host");
            var resource    = GetRequiredParam(parameters, "resource");
            var sapClient   = GetStringParam(parameters, "sapClient");

            _logger.LogInformation("SAP: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(host, parameters, config, resource, sapClient, filter: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SAP: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"SAP dry run preview failed for '{resource}': {ex.Message}", ex, "sap");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated via $skip/$top or __next) ───────────────────

        private async Task<List<object>> ReadFullAsync(
            string host,
            Dictionary<string, object> parameters,
            DataSourceConfig config,
            string resource,
            string? sapClient,
            string? filter,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var endpoint = ResolveEndpoint(resource);
            string? nextUrl = BuildUrl(host, endpoint, sapClient, filter, skip: 0);
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, parameters, config);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                if (!string.IsNullOrEmpty(sapClient))
                    request.Headers.TryAddWithoutValidation("sap-client", sapClient);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var (items, next) = ParseResultsPage(doc.RootElement);
                results.AddRange(items);

                // SAP OData: follow __next link or advance $skip
                if (!string.IsNullOrEmpty(next))
                {
                    nextUrl = next;
                }
                else if (items.Count >= PageSize)
                {
                    nextUrl = BuildUrl(host, endpoint, sapClient, filter, skip: results.Count);
                }
                else
                {
                    nextUrl = null;
                }

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("SAP: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string host, string endpoint, string? sapClient, string? filter, int skip)
        {
            var sb = new StringBuilder($"https://{host}{BasePath}{endpoint}?$top={PageSize}&$skip={skip}&$format=json");

            if (!string.IsNullOrEmpty(sapClient))
                sb.Append($"&sap-client={Uri.EscapeDataString(sapClient)}");

            if (!string.IsNullOrEmpty(filter))
                sb.Append($"&$filter={Uri.EscapeDataString(filter)}");

            return sb.ToString();
        }

        private static string ResolveEndpoint(string resource)
        {
            if (ResourceEndpoints.TryGetValue(resource, out var endpoint))
                return endpoint;

            // Assume the resource name is already a valid OData entity set path
            return resource;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses SAP OData JSON response: { "d": { "results": [...], "__next": "url" } }
        /// </summary>
        private static (List<object> Items, string? NextUrl) ParseResultsPage(JsonElement root)
        {
            var items = new List<object>();
            string? nextUrl = null;

            if (!root.TryGetProperty("d", out var d))
                return (items, nextUrl);

            // Single-entity responses have "d" as an object; collection responses have "d.results"
            JsonElement arrayElement;
            if (d.TryGetProperty("results", out var results) && results.ValueKind == JsonValueKind.Array)
            {
                arrayElement = results;
            }
            else if (d.ValueKind == JsonValueKind.Array)
            {
                arrayElement = d;
            }
            else
            {
                // Single entity — wrap it
                items.Add(FlattenElement(d));
                return (items, nextUrl);
            }

            // __next link for pagination
            if (d.TryGetProperty("__next", out var next) && next.ValueKind == JsonValueKind.String)
                nextUrl = next.GetString();

            foreach (var element in arrayElement.EnumerateArray())
            {
                items.Add(FlattenElement(element));
            }

            return (items, nextUrl);
        }

        private static object FlattenElement(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                // Skip OData metadata properties
                if (prop.Name.StartsWith("__"))
                    continue;

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

        private static void ApplyAuth(HttpRequestMessage request, Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var accessToken = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(accessToken))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                return;
            }

            var username = GetStringParam(parameters, "username");
            var password = GetStringParam(parameters, "password");
            if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
            {
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
                return;
            }

            // Fall back to connection string as plain token
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", config.ConnectionString);
                return;
            }

            throw new ConnectorException(
                "SAP authentication is required. Provide accessToken, or username+password via Parameters or the connection string.",
                new ArgumentException("Missing authentication credentials."),
                "sap");
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"host":"...","username":"...","password":"...","sapClient":"100"}
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
                    $"SAP connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "sap");
            return value;
        }
    }
}
