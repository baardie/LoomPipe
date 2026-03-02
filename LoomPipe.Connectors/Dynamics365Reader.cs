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
    /// Reads data from Dynamics 365 using the Web API (OData v4).
    ///
    /// Parameters:
    ///   accessToken  — Azure AD / Entra ID Bearer token
    ///   orgUrl       — Dynamics 365 org URL (e.g. "https://myorg.crm.dynamics.com")
    ///   resource     — contacts, accounts, leads, opportunities, incidents, invoices,
    ///                  quotes, orders, products, teams, systemusers, activities, tasks, campaigns
    ///
    /// ConnectionString JSON: {"orgUrl":"...","accessToken":"..."}
    /// Pagination: @odata.nextLink
    /// </summary>
    public class Dynamics365Reader : ISourceReader
    {
        private const string ApiPath = "/api/data/v9.2";
        private const int PageSize = 5000;

        private static readonly string[] AllResources =
        {
            "contacts", "accounts", "leads", "opportunities", "incidents",
            "invoices", "quotes", "orders", "products", "teams",
            "systemusers", "activities", "tasks", "campaigns"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<Dynamics365Reader> _logger;

        public Dynamics365Reader(HttpClient httpClient, ILogger<Dynamics365Reader> logger)
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
            var (orgUrl, accessToken) = ResolveCredentials(config);

            _logger.LogInformation("Dynamics365: reading resource '{Resource}'.", resource);

            try
            {
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    return await ReadPaginatedAsync(orgUrl, accessToken, resource, watermarkField, watermarkValue);
                }

                return await ReadPaginatedAsync(orgUrl, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dynamics365: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Dynamics 365 resource '{resource}': {ex.Message}", ex, "dynamics365");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (orgUrl, accessToken) = ResolveCredentials(config);

            _logger.LogInformation("Dynamics365: discovering schema for '{Resource}'.", resource);

            try
            {
                // Use EntityDefinitions to get attribute names.
                var entityName = NormalizeEntityName(resource);
                var url = $"{orgUrl.TrimEnd('/')}{ApiPath}/EntityDefinitions(LogicalName='{entityName}')/Attributes?$select=LogicalName";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);
                ApplyODataHeaders(request);

                using var response = await _httpClient.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    using var doc = JsonDocument.Parse(json);

                    var names = new List<string>();
                    if (doc.RootElement.TryGetProperty("value", out var value) && value.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var attr in value.EnumerateArray())
                        {
                            if (attr.TryGetProperty("LogicalName", out var name) && name.ValueKind == JsonValueKind.String)
                            {
                                names.Add(name.GetString()!);
                            }
                        }
                    }

                    return names;
                }

                // Fallback: read a sample and derive field names.
                var sample = (await ReadPaginatedAsync(orgUrl, accessToken, resource, maxPages: 1)).ToList();
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dynamics365: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Dynamics 365 schema for '{resource}': {ex.Message}", ex, "dynamics365");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (orgUrl, accessToken) = ResolveCredentials(config);

            _logger.LogInformation("Dynamics365: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(orgUrl, accessToken, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dynamics365: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Dynamics 365 dry run preview failed for '{resource}': {ex.Message}", ex, "dynamics365");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string orgUrl, string accessToken, string resource,
            string? watermarkField = null, string? watermarkValue = null,
            int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 0;

            var baseEndpoint = $"{orgUrl.TrimEnd('/')}{ApiPath}/{resource}?$top={PageSize}";

            // Add OData filter for incremental loads.
            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
            {
                baseEndpoint += $"&$filter={Uri.EscapeDataString(watermarkField)} gt {Uri.EscapeDataString(watermarkValue)}";
            }

            string? nextUrl = baseEndpoint;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);
                ApplyODataHeaders(request);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // OData response: { "value": [...], "@odata.nextLink": "..." }
                if (doc.RootElement.TryGetProperty("value", out var value) && value.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in value.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        if (element.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in element.EnumerateObject())
                            {
                                // Skip OData metadata annotations.
                                if (prop.Name.StartsWith("@") || prop.Name.StartsWith("_") && prop.Name.EndsWith("_value"))
                                    continue;

                                row[prop.Name] = ConvertJsonValue(prop.Value);
                            }

                            // Include lookup value fields (e.g. _parentcustomerid_value).
                            foreach (var prop in element.EnumerateObject())
                            {
                                if (prop.Name.StartsWith("_") && prop.Name.EndsWith("_value"))
                                {
                                    // Strip leading underscore and trailing _value for a cleaner name.
                                    var cleanName = prop.Name[1..^6];
                                    if (!row.ContainsKey(cleanName))
                                    {
                                        row[cleanName] = ConvertJsonValue(prop.Value);
                                    }
                                }
                            }
                        }

                        results.Add(row);
                    }
                }

                // @odata.nextLink pagination.
                nextUrl = null;
                if (doc.RootElement.TryGetProperty("@odata.nextLink", out var nextLink)
                    && nextLink.ValueKind == JsonValueKind.String)
                {
                    nextUrl = nextLink.GetString();
                    if (string.IsNullOrEmpty(nextUrl))
                        nextUrl = null;
                }

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Dynamics365: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Entity name normalisation ────────────────────────────────────────

        /// <summary>
        /// Normalises the resource name to the Dynamics 365 logical entity name
        /// (singular form used for EntityDefinitions).
        /// </summary>
        private static string NormalizeEntityName(string resource) => resource switch
        {
            "contacts"      => "contact",
            "accounts"      => "account",
            "leads"         => "lead",
            "opportunities" => "opportunity",
            "incidents"     => "incident",
            "invoices"      => "invoice",
            "quotes"        => "quote",
            "orders"        => "salesorder",
            "products"      => "product",
            "teams"         => "team",
            "systemusers"   => "systemuser",
            "activities"    => "activitypointer",
            "tasks"         => "task",
            "campaigns"     => "campaign",
            _               => resource.TrimEnd('s')
        };

        // ── Response parsing ─────────────────────────────────────────────────

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

        // ── Auth & header helpers ────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        private static void ApplyODataHeaders(HttpRequestMessage request)
        {
            request.Headers.TryAddWithoutValidation("OData-MaxVersion", "4.0");
            request.Headers.TryAddWithoutValidation("OData-Version", "4.0");
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Headers.TryAddWithoutValidation("Prefer", "odata.include-annotations=\"*\"");
        }

        // ── Credential resolution ────────────────────────────────────────────

        private (string orgUrl, string accessToken) ResolveCredentials(DataSourceConfig config)
        {
            var orgUrl = GetStringParam(config.Parameters, "orgUrl");
            var accessToken = GetStringParam(config.Parameters, "accessToken");

            // Fall back to ConnectionString JSON: {"orgUrl":"...","accessToken":"..."}
            if (string.IsNullOrEmpty(orgUrl) || string.IsNullOrEmpty(accessToken))
            {
                if (!string.IsNullOrWhiteSpace(config.ConnectionString))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(config.ConnectionString);
                        if (string.IsNullOrEmpty(orgUrl) && doc.RootElement.TryGetProperty("orgUrl", out var ou))
                            orgUrl = ou.GetString();
                        if (string.IsNullOrEmpty(accessToken) && doc.RootElement.TryGetProperty("accessToken", out var at))
                            accessToken = at.GetString();
                    }
                    catch (JsonException)
                    {
                        // ConnectionString is not valid JSON; treat as plain access token.
                        accessToken ??= config.ConnectionString;
                    }
                }
            }

            if (string.IsNullOrEmpty(orgUrl))
                throw new ConnectorException(
                    "Dynamics 365 org URL is required. Provide it via Parameters['orgUrl'] or the connection string.",
                    new ArgumentException("Missing 'orgUrl'."),
                    "dynamics365");

            if (string.IsNullOrEmpty(accessToken))
                throw new ConnectorException(
                    "Dynamics 365 access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "dynamics365");

            return (orgUrl, accessToken);
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
                    $"Dynamics 365 connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "dynamics365");
            return value;
        }
    }
}
