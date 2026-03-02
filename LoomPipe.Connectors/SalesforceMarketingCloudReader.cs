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
    /// Reads data from Salesforce Marketing Cloud using the REST API.
    ///
    /// Parameters:
    ///   accessToken   — Salesforce MC OAuth access token
    ///   subdomain     — MC REST API subdomain (e.g. "mc1234abcdef")
    ///   resource      — contacts, data_extensions, emails, sends, journeys,
    ///                   automations, lists, subscribers, events, content
    ///   clientId      — OAuth client ID (for token refresh)
    ///   clientSecret  — OAuth client secret (for token refresh)
    ///   dataExtensionKey — external key for data_extensions resource
    /// </summary>
    public class SalesforceMarketingCloudReader : ISourceReader
    {
        private const int DefaultPageSize = 100;

        private static readonly string[] AllResources =
        {
            "contacts", "data_extensions", "emails", "sends", "journeys",
            "automations", "lists", "subscribers", "events", "content"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SalesforceMarketingCloudReader> _logger;

        public SalesforceMarketingCloudReader(HttpClient httpClient, ILogger<SalesforceMarketingCloudReader> logger)
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
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = await GetAccessTokenAsync(parameters, config);
            var subdomain   = GetRequiredParam(parameters, "subdomain");

            _logger.LogInformation("SalesforceMC: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(parameters, resource, accessToken, subdomain);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SalesforceMC: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Salesforce MC resource '{resource}': {ex.Message}", ex, "salesforcemc");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = await GetAccessTokenAsync(parameters, config);
            var subdomain   = GetRequiredParam(parameters, "subdomain");

            _logger.LogInformation("SalesforceMC: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(parameters, resource, accessToken, subdomain, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SalesforceMC: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Salesforce MC schema for '{resource}': {ex.Message}", ex, "salesforcemc");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = await GetAccessTokenAsync(parameters, config);
            var subdomain   = GetRequiredParam(parameters, "subdomain");

            _logger.LogInformation("SalesforceMC: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(parameters, resource, accessToken, subdomain, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SalesforceMC: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Salesforce MC dry run preview failed for '{resource}': {ex.Message}", ex, "salesforcemc");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated) ────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            Dictionary<string, object> parameters, string resource,
            string accessToken, string subdomain, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            var baseUrl = $"https://{subdomain}.rest.marketingcloudapis.com";
            int currentPage = 1;
            int page = 0;
            bool hasMore = true;

            do
            {
                var url = BuildUrl(baseUrl, resource, parameters, currentPage);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var countBefore = results.Count;
                ParseResultsPage(doc.RootElement, resource, results);
                var added = results.Count - countBefore;

                // Determine pagination
                hasMore = false;
                if (doc.RootElement.TryGetProperty("count", out var countEl) && countEl.ValueKind == JsonValueKind.Number)
                {
                    var totalCount = countEl.GetInt32();
                    if (currentPage * DefaultPageSize < totalCount)
                    {
                        hasMore = true;
                    }
                }
                else if (doc.RootElement.TryGetProperty("page", out var pageEl)
                    && pageEl.TryGetProperty("pageSize", out var pageSizeEl))
                {
                    // Some endpoints use page metadata
                    if (added >= DefaultPageSize)
                    {
                        hasMore = true;
                    }
                }
                else if (added >= DefaultPageSize)
                {
                    hasMore = true;
                }

                currentPage++;
                page++;
            }
            while (hasMore && page < maxPages);

            _logger.LogInformation("SalesforceMC: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string baseUrl, string resource,
            Dictionary<string, object> parameters, int page)
        {
            return resource switch
            {
                "contacts"        => $"{baseUrl}/contacts/v1/contacts?$page={page}&$pageSize={DefaultPageSize}",
                "data_extensions" => BuildDataExtensionUrl(baseUrl, parameters, page),
                "emails"          => $"{baseUrl}/asset/v1/content/assets?$page={page}&$pageSize={DefaultPageSize}&$filter=assetType.name=htmlemail",
                "sends"           => $"{baseUrl}/messaging/v1/messageDefinitionSends?$page={page}&$pageSize={DefaultPageSize}",
                "journeys"        => $"{baseUrl}/interaction/v1/interactions?$page={page}&$pageSize={DefaultPageSize}",
                "automations"     => $"{baseUrl}/automation/v1/automations?$page={page}&$pageSize={DefaultPageSize}",
                "lists"           => $"{baseUrl}/contacts/v1/lists?$page={page}&$pageSize={DefaultPageSize}",
                "subscribers"     => $"{baseUrl}/address/v1/addresses?$page={page}&$pageSize={DefaultPageSize}",
                "events"          => $"{baseUrl}/interaction/v1/events?$page={page}&$pageSize={DefaultPageSize}",
                "content"         => $"{baseUrl}/asset/v1/content/assets?$page={page}&$pageSize={DefaultPageSize}",
                _                 => throw new ConnectorException(
                    $"Unsupported Salesforce MC resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "salesforcemc")
            };
        }

        private static string BuildDataExtensionUrl(string baseUrl,
            Dictionary<string, object> parameters, int page)
        {
            var key = GetStringParam(parameters, "dataExtensionKey");
            if (string.IsNullOrWhiteSpace(key))
                throw new ConnectorException(
                    "Salesforce MC data_extensions resource requires the 'dataExtensionKey' parameter.",
                    new ArgumentException("Missing 'dataExtensionKey'."),
                    "salesforcemc");

            return $"{baseUrl}/data/v1/customobjectdata/key/{Uri.EscapeDataString(key)}/rowset?$page={page}&$pageSize={DefaultPageSize}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            JsonElement items = default;
            bool found = false;

            // Try common collection property names
            foreach (var key in new[] { "items", "results", "entities", "value", "count" })
            {
                if (root.TryGetProperty(key, out var candidate) && candidate.ValueKind == JsonValueKind.Array)
                {
                    items = candidate;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                // data_extensions uses top-level array or "items" in the response
                if (root.ValueKind == JsonValueKind.Array)
                {
                    items = root;
                    found = true;
                }
            }

            if (!found)
                return;

            foreach (var element in items.EnumerateArray())
            {
                results.Add(FlattenElement(element));
            }
        }

        private static object FlattenElement(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
            }

            return row;
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True   => true,
                JsonValueKind.False  => false,
                JsonValueKind.Null   => string.Empty,
                JsonValueKind.Object => value.ToString(),
                JsonValueKind.Array  => value.ToString(),
                _                    => value.ToString()
            };
        }

        // ── Auth helpers ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        /// <summary>
        /// Retrieves an access token. If an accessToken parameter is already provided, uses it directly.
        /// Otherwise, attempts to obtain one via the OAuth2 client_credentials flow using clientId/clientSecret.
        /// </summary>
        private async Task<string> GetAccessTokenAsync(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (!string.IsNullOrWhiteSpace(token))
                return token;

            // Attempt token refresh via client_credentials
            var clientId     = GetStringParam(parameters, "clientId");
            var clientSecret = GetStringParam(parameters, "clientSecret");
            var subdomain    = GetStringParam(parameters, "subdomain");

            if (string.IsNullOrWhiteSpace(clientId) || string.IsNullOrWhiteSpace(clientSecret) || string.IsNullOrWhiteSpace(subdomain))
                throw new ConnectorException(
                    "Salesforce MC access token is required. Provide it via Parameters['accessToken'] or supply clientId + clientSecret for OAuth.",
                    new ArgumentException("Missing 'accessToken'."),
                    "salesforcemc");

            var tokenUrl = $"https://{subdomain}.auth.marketingcloudapis.com/v2/token";
            var body = JsonSerializer.Serialize(new
            {
                grant_type = "client_credentials",
                client_id = clientId,
                client_secret = clientSecret
            });

            using var request = new HttpRequestMessage(HttpMethod.Post, tokenUrl)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("access_token", out var accessTokenEl)
                && accessTokenEl.ValueKind == JsonValueKind.String)
            {
                return accessTokenEl.GetString()!;
            }

            throw new ConnectorException(
                "Failed to obtain Salesforce MC access token from OAuth response.",
                new InvalidOperationException("Missing 'access_token' in token response."),
                "salesforcemc");
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"subdomain":"...","clientId":"...","clientSecret":"..."}
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
            catch (JsonException) { /* not JSON — ignore */ }
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

        private static string GetRequiredParam(Dictionary<string, object> parameters, string key)
        {
            var value = GetStringParam(parameters, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"Salesforce MC connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "salesforcemc");
            return value;
        }
    }
}
