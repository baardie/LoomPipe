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
    /// Reads data from Google Drive using the v3 API.
    ///
    /// Parameters:
    ///   accessToken  — Google OAuth2 access token
    ///   resource     — files, folders, shared_drives, comments, permissions, revisions
    ///   folderId     — optional folder ID to filter files by parent
    ///   query        — optional Drive search query (q parameter)
    /// </summary>
    public class GoogleDriveReader : ISourceReader
    {
        private const string BaseUrl = "https://www.googleapis.com/drive/v3";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "files", "folders", "shared_drives", "comments", "permissions", "revisions"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GoogleDriveReader> _logger;

        public GoogleDriveReader(HttpClient httpClient, ILogger<GoogleDriveReader> logger)
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
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Google Drive access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googledrive");

            _logger.LogInformation("GoogleDrive: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(config, resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleDrive: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Google Drive resource '{resource}': {ex.Message}", ex, "googledrive");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Google Drive access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googledrive");

            _logger.LogInformation("GoogleDrive: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleDrive: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Google Drive schema for '{resource}': {ex.Message}", ex, "googledrive");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Google Drive access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "googledrive");

            _logger.LogInformation("GoogleDrive: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(config, resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GoogleDrive: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Google Drive dry run preview failed for '{resource}': {ex.Message}", ex, "googledrive");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (pageToken-based pagination) ───────────────────────────

        private async Task<List<object>> ReadFullAsync(
            DataSourceConfig config, string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? pageToken = null;
            int page = 0;

            do
            {
                var url = BuildUrl(config, resource, pageToken);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse the items array based on resource type
                var arrayProp = GetArrayProperty(resource);
                if (root.TryGetProperty(arrayProp, out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in items.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }
                else if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }

                // Pagination
                pageToken = null;
                if (root.TryGetProperty("nextPageToken", out var npt) && npt.ValueKind == JsonValueKind.String)
                {
                    pageToken = npt.GetString();
                }

                page++;
            }
            while (pageToken != null && page < maxPages);

            _logger.LogInformation("GoogleDrive: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builders ─────────────────────────────────────────────────────

        private static string BuildUrl(DataSourceConfig config, string resource, string? pageToken)
        {
            var folderId = GetStringParam(config.Parameters, "folderId");
            var query    = GetStringParam(config.Parameters, "query");

            var sb = new StringBuilder();

            switch (resource)
            {
                case "files":
                    sb.Append($"{BaseUrl}/files?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,files(id,name,mimeType,size,createdTime,modifiedTime,parents,owners)");

                    // Build query filters
                    var qParts = new List<string>();
                    if (!string.IsNullOrEmpty(folderId))
                        qParts.Add($"'{folderId}' in parents");
                    if (!string.IsNullOrEmpty(query))
                        qParts.Add(query);
                    if (qParts.Count > 0)
                        sb.Append($"&q={Uri.EscapeDataString(string.Join(" and ", qParts))}");
                    break;

                case "folders":
                    sb.Append($"{BaseUrl}/files?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,files(id,name,mimeType,createdTime,modifiedTime,parents)");
                    var folderQuery = "mimeType='application/vnd.google-apps.folder'";
                    if (!string.IsNullOrEmpty(folderId))
                        folderQuery += $" and '{folderId}' in parents";
                    sb.Append($"&q={Uri.EscapeDataString(folderQuery)}");
                    break;

                case "shared_drives":
                    sb.Append($"{BaseUrl}/drives?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,drives(id,name,createdTime,hidden,restrictions)");
                    break;

                case "comments":
                {
                    // Comments require a fileId — use folderId as fileId for this purpose.
                    var fileId = folderId
                        ?? throw new ConnectorException(
                            "Google Drive 'comments' resource requires the 'folderId' parameter (used as fileId).",
                            new ArgumentException("Missing required parameter: folderId"),
                            "googledrive");
                    sb.Append($"{BaseUrl}/files/{Uri.EscapeDataString(fileId)}/comments?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,comments(id,author,content,createdTime,modifiedTime,resolved)");
                    break;
                }

                case "permissions":
                {
                    var fileId = folderId
                        ?? throw new ConnectorException(
                            "Google Drive 'permissions' resource requires the 'folderId' parameter (used as fileId).",
                            new ArgumentException("Missing required parameter: folderId"),
                            "googledrive");
                    sb.Append($"{BaseUrl}/files/{Uri.EscapeDataString(fileId)}/permissions?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,permissions(id,type,role,emailAddress,displayName)");
                    break;
                }

                case "revisions":
                {
                    var fileId = folderId
                        ?? throw new ConnectorException(
                            "Google Drive 'revisions' resource requires the 'folderId' parameter (used as fileId).",
                            new ArgumentException("Missing required parameter: folderId"),
                            "googledrive");
                    sb.Append($"{BaseUrl}/files/{Uri.EscapeDataString(fileId)}/revisions?pageSize={PageLimit}");
                    sb.Append("&fields=nextPageToken,revisions(id,mimeType,modifiedTime,keepForever,published,size)");
                    break;
                }

                default:
                    throw new ConnectorException(
                        $"Unknown Google Drive resource '{resource}'.",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "googledrive");
            }

            if (!string.IsNullOrEmpty(pageToken))
                sb.Append($"&pageToken={Uri.EscapeDataString(pageToken)}");

            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON array property name for the given resource type.
        /// </summary>
        private static string GetArrayProperty(string resource) => resource switch
        {
            "files"         => "files",
            "folders"       => "files",
            "shared_drives" => "drives",
            "comments"      => "comments",
            "permissions"   => "permissions",
            "revisions"     => "revisions",
            _               => "files"
        };

        // ── JSON flattening ─────────────────────────────────────────────────

        private static object FlattenJsonObject(JsonElement element)
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

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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
                    $"Google Drive connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "googledrive");
            return value;
        }
    }
}
