#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
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
    /// <summary>
    /// Reads data from SharePoint Online via Microsoft Graph API (v1.0).
    ///
    /// Parameters:
    ///   accessToken — Azure AD / Entra ID Bearer token
    ///   resource    — sites, lists, list_items, drives, drive_items, pages, columns
    ///   siteId      — SharePoint site ID (required for lists, list_items, drives, drive_items, pages, columns)
    ///   listId      — SharePoint list ID (required for list_items, columns)
    ///   driveId     — SharePoint drive ID (optional for drive_items)
    ///   searchQuery — search query for sites resource
    /// </summary>
    public class SharePointReader : ISourceReader
    {
        private const string BaseUrl = "https://graph.microsoft.com/v1.0";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "sites", "lists", "list_items", "drives", "drive_items", "pages", "columns"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SharePointReader> _logger;

        public SharePointReader(HttpClient httpClient, ILogger<SharePointReader> logger)
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
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SharePoint: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(parameters, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SharePoint: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read SharePoint resource '{resource}': {ex.Message}", ex, "sharepoint");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SharePoint: discovering schema for '{Resource}'.", resource);

            try
            {
                // For columns resource, fetch column definitions directly
                if (resource == "columns")
                {
                    return await FetchColumnNamesAsync(parameters, accessToken);
                }

                var sample = await ReadFullAsync(parameters, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SharePoint: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover SharePoint schema for '{resource}': {ex.Message}", ex, "sharepoint");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("SharePoint: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(parameters, resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SharePoint: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"SharePoint dry run preview failed for '{resource}': {ex.Message}", ex, "sharepoint");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            Dictionary<string, object> parameters, string resource,
            string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextLink = null;
            int page = 0;

            do
            {
                var url = nextLink ?? BuildUrl(resource, parameters);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsPage(doc.RootElement, resource, results);

                // Pagination via @odata.nextLink
                nextLink = null;
                if (doc.RootElement.TryGetProperty("@odata.nextLink", out var nextLinkEl)
                    && nextLinkEl.ValueKind == JsonValueKind.String)
                {
                    nextLink = nextLinkEl.GetString();
                }

                page++;
            }
            while (nextLink != null && page < maxPages);

            _logger.LogInformation("SharePoint: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, Dictionary<string, object> parameters)
        {
            var siteId  = GetStringParam(parameters, "siteId");
            var listId  = GetStringParam(parameters, "listId");
            var driveId = GetStringParam(parameters, "driveId");
            var searchQuery = GetStringParam(parameters, "searchQuery");

            return resource switch
            {
                "sites"       => !string.IsNullOrWhiteSpace(searchQuery)
                    ? $"{BaseUrl}/sites?search={Uri.EscapeDataString(searchQuery)}&$top={PageSize}"
                    : $"{BaseUrl}/sites?$top={PageSize}",
                "lists"       => $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/lists?$top={PageSize}",
                "list_items"  => $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/lists/{EscapeId(listId, "listId", "sharepoint")}/items?expand=fields&$top={PageSize}",
                "drives"      => $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/drives?$top={PageSize}",
                "drive_items" => BuildDriveItemsUrl(siteId, driveId),
                "pages"       => $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/pages?$top={PageSize}",
                "columns"     => $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/lists/{EscapeId(listId, "listId", "sharepoint")}/columns?$top={PageSize}",
                _             => throw new ConnectorException(
                    $"Unsupported SharePoint resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "sharepoint")
            };
        }

        private static string BuildDriveItemsUrl(string? siteId, string? driveId)
        {
            var site = EscapeId(siteId, "siteId", "sharepoint");
            if (!string.IsNullOrWhiteSpace(driveId))
                return $"{BaseUrl}/sites/{site}/drives/{Uri.EscapeDataString(driveId)}/root/children?$top={PageSize}";
            return $"{BaseUrl}/sites/{site}/drive/root/children?$top={PageSize}";
        }

        private static string EscapeId(string? id, string paramName, string provider)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ConnectorException(
                    $"SharePoint connector requires the '{paramName}' parameter for this resource.",
                    new ArgumentException($"Missing required parameter: {paramName}"),
                    provider);
            return Uri.EscapeDataString(id);
        }

        // ── Column name discovery ────────────────────────────────────────────

        private async Task<List<string>> FetchColumnNamesAsync(
            Dictionary<string, object> parameters, string accessToken)
        {
            var siteId = GetStringParam(parameters, "siteId");
            var listId = GetStringParam(parameters, "listId");
            var url = $"{BaseUrl}/sites/{EscapeId(siteId, "siteId", "sharepoint")}/lists/{EscapeId(listId, "listId", "sharepoint")}/columns";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var names = new List<string>();
            if (doc.RootElement.TryGetProperty("value", out var value)
                && value.ValueKind == JsonValueKind.Array)
            {
                foreach (var col in value.EnumerateArray())
                {
                    if (col.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                    {
                        names.Add(name.GetString()!);
                    }
                }
            }

            return names;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("value", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Graph API response shape
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                // For list_items, flatten the "fields" sub-object
                if (resource == "list_items" && element.TryGetProperty("fields", out var fields)
                    && fields.ValueKind == JsonValueKind.Object)
                {
                    // Include the item id
                    if (element.TryGetProperty("id", out var id))
                        row["id"] = id.GetString() ?? id.ToString();

                    foreach (var prop in fields.EnumerateObject())
                    {
                        // Skip @odata metadata properties
                        if (prop.Name.StartsWith("@odata"))
                            continue;
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }
                else
                {
                    foreach (var prop in element.EnumerateObject())
                    {
                        // Skip @odata metadata properties
                        if (prop.Name.StartsWith("@odata"))
                            continue;
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
            }
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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"accessToken":"...","siteId":"...","listId":"..."}
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

        private static string GetAccessToken(Dictionary<string, object> parameters, DataSourceConfig config)
        {
            var token = GetStringParam(parameters, "accessToken");
            if (string.IsNullOrWhiteSpace(token))
                throw new ConnectorException(
                    "SharePoint access token is required. Provide it via Parameters['accessToken'] or the connection string JSON.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sharepoint");
            return token;
        }

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
                    $"SharePoint connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "sharepoint");
            return value;
        }
    }
}
