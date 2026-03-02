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
    /// Reads documents from Elasticsearch using the REST API.
    ///
    /// Parameters:
    ///   host      — Elasticsearch host (e.g. "localhost" or "https://my-cluster.es.io")
    ///   port      — Elasticsearch port (default 9200)
    ///   username  — Basic auth username (optional)
    ///   password  — Basic auth password (optional)
    ///   apiKey    — API key for authentication (optional, alternative to basic auth)
    ///   index     — Index name to read from
    ///   query     — Optional Elasticsearch DSL JSON query
    /// </summary>
    public class ElasticsearchReader : ISourceReader
    {
        private const int DefaultPageSize = 1000;
        private const int MaxFromSize = 10000;

        private static readonly string[] AllResources = Array.Empty<string>(); // Discovered dynamically via _cat/indices

        private readonly HttpClient _httpClient;
        private readonly ILogger<ElasticsearchReader> _logger;

        public ElasticsearchReader(HttpClient httpClient, ILogger<ElasticsearchReader> logger)
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
            var parameters = MergeConnectionString(config);
            var index      = GetRequiredParam(parameters, "index");
            var baseUrl    = BuildBaseUrl(parameters);

            _logger.LogInformation("Elasticsearch: reading index '{Index}'.", index);

            try
            {
                var queryDsl = BuildQueryDsl(parameters, watermarkField, watermarkValue);
                return await SearchAsync(baseUrl, parameters, index, queryDsl);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elasticsearch: failed to read index '{Index}'.", index);
                throw new ConnectorException($"Failed to read Elasticsearch index '{index}': {ex.Message}", ex, "elasticsearch");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var index      = GetRequiredParam(parameters, "index");
            var baseUrl    = BuildBaseUrl(parameters);

            _logger.LogInformation("Elasticsearch: discovering schema for index '{Index}'.", index);

            try
            {
                var url = $"{baseUrl}/{Uri.EscapeDataString(index)}/_mapping";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var names = new List<string>();

                // Response shape: { "index_name": { "mappings": { "properties": { "field": {...} } } } }
                foreach (var indexEntry in doc.RootElement.EnumerateObject())
                {
                    if (indexEntry.Value.TryGetProperty("mappings", out var mappings)
                        && mappings.TryGetProperty("properties", out var properties)
                        && properties.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var prop in properties.EnumerateObject())
                        {
                            names.Add(prop.Name);
                        }
                        break; // Only need the first index mapping
                    }
                }

                return names;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elasticsearch: failed to discover schema for index '{Index}'.", index);
                throw new ConnectorException($"Failed to discover Elasticsearch schema for '{index}': {ex.Message}", ex, "elasticsearch");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var index      = GetRequiredParam(parameters, "index");
            var baseUrl    = BuildBaseUrl(parameters);

            _logger.LogInformation("Elasticsearch: dry run preview for index '{Index}' (sample={SampleSize}).", index, sampleSize);

            try
            {
                var url = $"{baseUrl}/{Uri.EscapeDataString(index)}/_search";
                var body = JsonSerializer.Serialize(new { size = sampleSize, query = new { match_all = new object() } });

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                return ParseHits(doc.RootElement);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Elasticsearch: dry run preview failed for index '{Index}'.", index);
                throw new ConnectorException($"Elasticsearch dry run preview failed for '{index}': {ex.Message}", ex, "elasticsearch");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var baseUrl    = BuildBaseUrl(parameters);

            try
            {
                var url = $"{baseUrl}/_cat/indices?format=json";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var indices = new List<string>();
                if (doc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var entry in doc.RootElement.EnumerateArray())
                    {
                        if (entry.TryGetProperty("index", out var indexName)
                            && indexName.ValueKind == JsonValueKind.String)
                        {
                            var name = indexName.GetString();
                            if (!string.IsNullOrEmpty(name) && !name.StartsWith("."))
                                indices.Add(name);
                        }
                    }
                }

                return indices;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Elasticsearch: failed to list indices; returning empty list.");
                return Array.Empty<string>();
            }
        }

        // ── Search with pagination ───────────────────────────────────────────

        private async Task<List<object>> SearchAsync(
            string baseUrl, Dictionary<string, object> parameters,
            string index, string queryDsl)
        {
            var results = new List<object>();
            var url = $"{baseUrl}/{Uri.EscapeDataString(index)}/_search";
            int from = 0;
            JsonElement[]? searchAfter = null;

            while (true)
            {
                string body;
                if (from < MaxFromSize || searchAfter == null)
                {
                    // Use from+size pagination for the first 10,000 results
                    body = BuildSearchBody(queryDsl, DefaultPageSize, from, searchAfter: null);
                }
                else
                {
                    // Switch to search_after for deep pagination
                    body = BuildSearchBody(queryDsl, DefaultPageSize, from: null, searchAfter);
                }

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, parameters);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var hits = ParseHits(doc.RootElement);
                if (hits.Count == 0)
                    break;

                results.AddRange(hits);

                // Extract the last sort value for search_after
                searchAfter = ExtractLastSort(doc.RootElement);

                from += hits.Count;

                if (hits.Count < DefaultPageSize)
                    break;
            }

            _logger.LogInformation("Elasticsearch: read {Count} documents from index '{Index}'.",
                results.Count, index);

            return results;
        }

        private static string BuildSearchBody(string queryDsl, int size, int? from, JsonElement[]? searchAfter)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteNumber("size", size);

            if (from.HasValue)
            {
                writer.WriteNumber("from", from.Value);
            }

            // Query
            writer.WritePropertyName("query");
            using var queryDoc = JsonDocument.Parse(queryDsl);
            queryDoc.RootElement.WriteTo(writer);

            // Sort (needed for search_after)
            writer.WritePropertyName("sort");
            writer.WriteStartArray();
            writer.WriteStartObject();
            writer.WriteString("_doc", "asc");
            writer.WriteEndObject();
            writer.WriteEndArray();

            // search_after
            if (searchAfter != null && searchAfter.Length > 0)
            {
                writer.WritePropertyName("search_after");
                writer.WriteStartArray();
                foreach (var val in searchAfter)
                {
                    val.WriteTo(writer);
                }
                writer.WriteEndArray();
            }

            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static string BuildQueryDsl(Dictionary<string, object> parameters,
            string? watermarkField, string? watermarkValue)
        {
            var customQuery = GetStringParam(parameters, "query");

            if (!string.IsNullOrWhiteSpace(customQuery))
            {
                // If a watermark is provided, wrap the custom query with a bool filter
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    return JsonSerializer.Serialize(new
                    {
                        @bool = new
                        {
                            must = JsonSerializer.Deserialize<object>(customQuery),
                            filter = new[]
                            {
                                new { range = new Dictionary<string, object>
                                {
                                    [watermarkField] = new { gt = watermarkValue }
                                }}
                            }
                        }
                    });
                }
                return customQuery;
            }

            if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
            {
                return JsonSerializer.Serialize(new
                {
                    range = new Dictionary<string, object>
                    {
                        [watermarkField] = new { gt = watermarkValue }
                    }
                });
            }

            return "{\"match_all\":{}}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static List<object> ParseHits(JsonElement root)
        {
            var results = new List<object>();

            if (!root.TryGetProperty("hits", out var hitsOuter)
                || !hitsOuter.TryGetProperty("hits", out var hitsArray)
                || hitsArray.ValueKind != JsonValueKind.Array)
            {
                return results;
            }

            foreach (var hit in hitsArray.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                // Include metadata fields
                if (hit.TryGetProperty("_id", out var id))
                    row["_id"] = id.GetString() ?? id.ToString();
                if (hit.TryGetProperty("_index", out var idx))
                    row["_index"] = idx.GetString() ?? idx.ToString();
                if (hit.TryGetProperty("_score", out var score))
                    row["_score"] = score.ValueKind == JsonValueKind.Number
                        ? (object)(score.TryGetDouble(out var d) ? d : 0)
                        : string.Empty;

                // Flatten _source into the row
                if (hit.TryGetProperty("_source", out var source) && source.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in source.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
            }

            return results;
        }

        private static JsonElement[]? ExtractLastSort(JsonElement root)
        {
            if (!root.TryGetProperty("hits", out var hitsOuter)
                || !hitsOuter.TryGetProperty("hits", out var hitsArray)
                || hitsArray.ValueKind != JsonValueKind.Array
                || hitsArray.GetArrayLength() == 0)
            {
                return null;
            }

            var lastHit = hitsArray[hitsArray.GetArrayLength() - 1];
            if (lastHit.TryGetProperty("sort", out var sort) && sort.ValueKind == JsonValueKind.Array)
            {
                return sort.EnumerateArray().Select(e => e.Clone()).ToArray();
            }

            return null;
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

        private static void ApplyAuth(HttpRequestMessage request, Dictionary<string, object> parameters)
        {
            var apiKey   = GetStringParam(parameters, "apiKey");
            var username = GetStringParam(parameters, "username");
            var password = GetStringParam(parameters, "password");

            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("ApiKey", apiKey);
            }
            else if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
            {
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            }
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildBaseUrl(Dictionary<string, object> parameters)
        {
            var host = GetStringParam(parameters, "host") ?? "localhost";
            var portStr = GetStringParam(parameters, "port") ?? "9200";

            // If host already includes protocol, use as-is
            if (host.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
                || host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                return host.TrimEnd('/');
            }

            return $"http://{host}:{portStr}";
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"host":"...","port":9200,"username":"...","password":"..."}
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
                    $"Elasticsearch connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "elasticsearch");
            return value;
        }
    }
}
