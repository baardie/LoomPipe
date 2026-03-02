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
    /// Reads data from Google Firestore using the REST API v1.
    ///
    /// Parameters:
    ///   accessToken  — Firebase/Google OAuth token or API key
    ///   projectId    — Google Cloud project ID
    ///   collection   — Firestore collection name
    ///   databaseId   — optional database ID (default: "(default)")
    ///   resource     — collections (list collections) or documents (list/query documents)
    /// </summary>
    public class FirebaseReader : ISourceReader
    {
        private const string BaseUrl = "https://firestore.googleapis.com/v1";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "collections", "documents"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<FirebaseReader> _logger;

        public FirebaseReader(HttpClient httpClient, ILogger<FirebaseReader> logger)
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
                    "Firebase access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "firebase");

            _logger.LogInformation("Firebase: reading resource '{Resource}'.", resource);

            try
            {
                return resource switch
                {
                    "collections" => await ReadCollectionsAsync(config, accessToken),
                    "documents"   => await ReadDocumentsAsync(config, accessToken),
                    _             => throw new ConnectorException(
                        $"Unknown Firebase resource '{resource}'.",
                        new ArgumentException($"Unsupported resource: {resource}"),
                        "firebase")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Firebase: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Firebase resource '{resource}': {ex.Message}", ex, "firebase");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Firebase access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "firebase");

            _logger.LogInformation("Firebase: discovering schema for '{Resource}'.", resource);

            try
            {
                List<object> sample = resource switch
                {
                    "collections" => await ReadCollectionsAsync(config, accessToken),
                    "documents"   => await ReadDocumentsAsync(config, accessToken, maxPages: 1),
                    _             => new List<object>()
                };

                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Firebase: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Firebase schema for '{resource}': {ex.Message}", ex, "firebase");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Firebase access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "firebase");

            _logger.LogInformation("Firebase: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                List<object> records = resource switch
                {
                    "collections" => await ReadCollectionsAsync(config, accessToken),
                    "documents"   => await ReadDocumentsAsync(config, accessToken, maxPages: 1),
                    _             => new List<object>()
                };

                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Firebase: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Firebase dry run preview failed for '{resource}': {ex.Message}", ex, "firebase");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── List collections ─────────────────────────────────────────────────

        private async Task<List<object>> ReadCollectionsAsync(DataSourceConfig config, string accessToken)
        {
            var projectId  = GetRequiredParam(config.Parameters, "projectId");
            var databaseId = GetStringParam(config.Parameters, "databaseId") ?? "(default)";

            var url = $"{BaseUrl}/projects/{Uri.EscapeDataString(projectId)}/databases/{Uri.EscapeDataString(databaseId)}/documents:listCollectionIds";

            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent("{\"pageSize\":100}", System.Text.Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var results = new List<object>();
            if (root.TryGetProperty("collectionIds", out var ids) && ids.ValueKind == JsonValueKind.Array)
            {
                foreach (var id in ids.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["collectionId"] = id.GetString() ?? string.Empty;
                    results.Add(row);
                }
            }

            _logger.LogInformation("Firebase: discovered {Count} collections.", results.Count);
            return results;
        }

        // ── List documents (pageToken-based pagination) ─────────────────────

        private async Task<List<object>> ReadDocumentsAsync(
            DataSourceConfig config, string accessToken, int maxPages = int.MaxValue)
        {
            var projectId  = GetRequiredParam(config.Parameters, "projectId");
            var collection = GetRequiredParam(config.Parameters, "collection");
            var databaseId = GetStringParam(config.Parameters, "databaseId") ?? "(default)";

            var results = new List<object>();
            string? pageToken = null;
            int page = 0;

            do
            {
                var url = $"{BaseUrl}/projects/{Uri.EscapeDataString(projectId)}/databases/{Uri.EscapeDataString(databaseId)}/documents/{Uri.EscapeDataString(collection)}?pageSize={PageLimit}";
                if (!string.IsNullOrEmpty(pageToken))
                    url += $"&pageToken={Uri.EscapeDataString(pageToken)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Parse the "documents" array
                if (root.TryGetProperty("documents", out var documents) && documents.ValueKind == JsonValueKind.Array)
                {
                    foreach (var docEl in documents.EnumerateArray())
                        results.Add(FlattenFirestoreDocument(docEl));
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

            _logger.LogInformation("Firebase: read {Count} documents from '{Collection}' across {Pages} page(s).",
                results.Count, collection, page);

            return results;
        }

        // ── Firestore document flattening ───────────────────────────────────

        /// <summary>
        /// Flattens a Firestore document into a simple ExpandoObject.
        /// Extracts the document name (as _documentId) and flattens all fields
        /// by converting Firestore value types to simple .NET types.
        /// </summary>
        private static object FlattenFirestoreDocument(JsonElement document)
        {
            IDictionary<string, object> row = new ExpandoObject();

            // Extract document name/ID
            if (document.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
            {
                var fullPath = name.GetString() ?? string.Empty;
                // Extract the document ID from the full path (last segment)
                var lastSlash = fullPath.LastIndexOf('/');
                row["_documentId"] = lastSlash >= 0 ? fullPath.Substring(lastSlash + 1) : fullPath;
                row["_documentPath"] = fullPath;
            }

            // Extract createTime and updateTime
            if (document.TryGetProperty("createTime", out var createTime))
                row["_createTime"] = createTime.GetString() ?? string.Empty;
            if (document.TryGetProperty("updateTime", out var updateTime))
                row["_updateTime"] = updateTime.GetString() ?? string.Empty;

            // Flatten fields
            if (document.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Object)
            {
                foreach (var field in fields.EnumerateObject())
                {
                    row[field.Name] = ConvertFirestoreValue(field.Value);
                }
            }

            return row;
        }

        /// <summary>
        /// Converts a Firestore value type into a simple .NET type.
        /// Firestore values are wrapped in type objects like:
        ///   { "stringValue": "hello" }
        ///   { "integerValue": "42" }
        ///   { "booleanValue": true }
        ///   { "doubleValue": 3.14 }
        ///   { "timestampValue": "2024-01-01T00:00:00Z" }
        ///   { "nullValue": null }
        ///   { "mapValue": { "fields": { ... } } }
        ///   { "arrayValue": { "values": [ ... ] } }
        ///   { "geoPointValue": { "latitude": ..., "longitude": ... } }
        ///   { "referenceValue": "projects/.../documents/..." }
        ///   { "bytesValue": "base64..." }
        /// </summary>
        private static object ConvertFirestoreValue(JsonElement value)
        {
            if (value.ValueKind != JsonValueKind.Object)
                return value.ToString();

            if (value.TryGetProperty("stringValue", out var sv))
                return sv.GetString() ?? string.Empty;

            if (value.TryGetProperty("integerValue", out var iv))
            {
                var intStr = iv.GetString() ?? iv.ToString();
                return long.TryParse(intStr, out var l) ? (object)l : intStr;
            }

            if (value.TryGetProperty("doubleValue", out var dv))
                return dv.GetDouble();

            if (value.TryGetProperty("booleanValue", out var bv))
                return bv.GetBoolean();

            if (value.TryGetProperty("timestampValue", out var tv))
                return tv.GetString() ?? string.Empty;

            if (value.TryGetProperty("nullValue", out _))
                return string.Empty;

            if (value.TryGetProperty("referenceValue", out var rv))
                return rv.GetString() ?? string.Empty;

            if (value.TryGetProperty("bytesValue", out var bytesV))
                return bytesV.GetString() ?? string.Empty;

            if (value.TryGetProperty("geoPointValue", out var gv))
            {
                var lat = gv.TryGetProperty("latitude", out var latEl) ? latEl.GetDouble() : 0.0;
                var lng = gv.TryGetProperty("longitude", out var lngEl) ? lngEl.GetDouble() : 0.0;
                return $"{lat},{lng}";
            }

            if (value.TryGetProperty("mapValue", out var mv))
            {
                // Flatten nested map into a JSON string representation
                if (mv.TryGetProperty("fields", out var mapFields))
                    return mapFields.ToString();
                return string.Empty;
            }

            if (value.TryGetProperty("arrayValue", out var av))
            {
                // Convert array to a JSON string representation
                if (av.TryGetProperty("values", out var arrayValues))
                    return arrayValues.ToString();
                return string.Empty;
            }

            return value.ToString();
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
                    $"Firebase connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "firebase");
            return value;
        }
    }
}
