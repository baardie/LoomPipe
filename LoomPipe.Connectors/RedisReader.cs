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
    /// Reads data from Redis via REST API (Upstash-compatible).
    ///
    /// Parameters:
    ///   host        — Redis REST URL (e.g. "https://us1-example.upstash.io")
    ///   accessToken — REST API token (Bearer auth)
    ///   resource    — keys, hash, list, set, sorted_set, stream
    ///   pattern     — glob pattern for KEYS resource (default "*")
    ///   key         — specific key name (required for hash, list, set, sorted_set, stream)
    /// </summary>
    public class RedisReader : ISourceReader
    {
        private static readonly string[] AllResources =
        {
            "keys", "hash", "list", "set", "sorted_set", "stream"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<RedisReader> _logger;

        public RedisReader(HttpClient httpClient, ILogger<RedisReader> logger)
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
            var host        = GetRequiredParam(parameters, "host");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("Redis: reading resource '{Resource}'.", resource);

            try
            {
                return resource switch
                {
                    "keys"       => await ReadKeysAsync(host, accessToken, parameters),
                    "hash"       => await ReadHashAsync(host, accessToken, parameters),
                    "list"       => await ReadListAsync(host, accessToken, parameters),
                    "set"        => await ReadSetAsync(host, accessToken, parameters),
                    "sorted_set" => await ReadSortedSetAsync(host, accessToken, parameters),
                    "stream"     => await ReadStreamAsync(host, accessToken, parameters),
                    _            => throw new ConnectorException(
                        $"Unsupported Redis resource: '{resource}'.",
                        new ArgumentException($"Unknown resource: {resource}"),
                        "redis")
                };
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Redis: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Redis resource '{resource}': {ex.Message}", ex, "redis");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");
            var host        = GetRequiredParam(parameters, "host");
            var accessToken = GetAccessToken(parameters, config);

            _logger.LogInformation("Redis: discovering schema for '{Resource}'.", resource);

            try
            {
                // Read a sample and return field names from the first result
                var sample = await ReadAsync(config);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Redis: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Redis schema for '{resource}': {ex.Message}", ex, "redis");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters  = MergeConnectionString(config);
            var resource    = GetRequiredParam(parameters, "resource");

            _logger.LogInformation("Redis: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Redis: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Redis dry run preview failed for '{resource}': {ex.Message}", ex, "redis");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── KEYS resource ────────────────────────────────────────────────────

        private async Task<List<object>> ReadKeysAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var pattern = GetStringParam(parameters, "pattern") ?? "*";

            // SCAN 0 MATCH pattern COUNT 1000 (iterates to get all keys)
            var allKeys = new List<string>();
            string cursor = "0";

            do
            {
                var scanResult = await ExecuteCommandAsync(host, accessToken,
                    new object[] { "SCAN", cursor, "MATCH", pattern, "COUNT", "1000" });

                using var doc = JsonDocument.Parse(scanResult);
                if (doc.RootElement.TryGetProperty("result", out var result)
                    && result.ValueKind == JsonValueKind.Array
                    && result.GetArrayLength() >= 2)
                {
                    cursor = result[0].GetString() ?? "0";
                    if (result[1].ValueKind == JsonValueKind.Array)
                    {
                        foreach (var key in result[1].EnumerateArray())
                        {
                            var keyStr = key.GetString();
                            if (!string.IsNullOrEmpty(keyStr))
                                allKeys.Add(keyStr);
                        }
                    }
                }
                else
                {
                    break;
                }
            }
            while (cursor != "0");

            // Bulk GET values for each key
            var results = new List<object>();
            foreach (var key in allKeys)
            {
                var valueJson = await ExecuteCommandAsync(host, accessToken, new object[] { "GET", key });
                using var valueDoc = JsonDocument.Parse(valueJson);

                IDictionary<string, object> row = new ExpandoObject();
                row["key"] = key;

                if (valueDoc.RootElement.TryGetProperty("result", out var val))
                {
                    row["value"] = ConvertJsonValue(val);
                }
                else
                {
                    row["value"] = string.Empty;
                }

                results.Add(row);
            }

            _logger.LogInformation("Redis: read {Count} keys matching pattern '{Pattern}'.", results.Count, pattern);
            return results;
        }

        // ── HASH resource ────────────────────────────────────────────────────

        private async Task<List<object>> ReadHashAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var key = GetRequiredKeyParam(parameters);
            var json = await ExecuteCommandAsync(host, accessToken, new object[] { "HGETALL", key });
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("result", out var result))
            {
                IDictionary<string, object> row = new ExpandoObject();
                row["_key"] = key;

                if (result.ValueKind == JsonValueKind.Array)
                {
                    // HGETALL returns [field1, value1, field2, value2, ...]
                    var items = result.EnumerateArray().ToList();
                    for (int i = 0; i < items.Count - 1; i += 2)
                    {
                        var fieldName = items[i].GetString() ?? $"field_{i}";
                        row[fieldName] = ConvertJsonValue(items[i + 1]);
                    }
                }
                else if (result.ValueKind == JsonValueKind.Object)
                {
                    // Some REST APIs return as object directly
                    foreach (var prop in result.EnumerateObject())
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
            }

            _logger.LogInformation("Redis: read hash '{Key}'.", key);
            return results;
        }

        // ── LIST resource ────────────────────────────────────────────────────

        private async Task<List<object>> ReadListAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var key = GetRequiredKeyParam(parameters);
            var json = await ExecuteCommandAsync(host, accessToken, new object[] { "LRANGE", key, "0", "-1" });
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("result", out var result)
                && result.ValueKind == JsonValueKind.Array)
            {
                int index = 0;
                foreach (var item in result.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["_key"] = key;
                    row["_index"] = index;
                    row["value"] = ConvertJsonValue(item);
                    results.Add(row);
                    index++;
                }
            }

            _logger.LogInformation("Redis: read {Count} items from list '{Key}'.", results.Count, key);
            return results;
        }

        // ── SET resource ─────────────────────────────────────────────────────

        private async Task<List<object>> ReadSetAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var key = GetRequiredKeyParam(parameters);
            var json = await ExecuteCommandAsync(host, accessToken, new object[] { "SMEMBERS", key });
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("result", out var result)
                && result.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in result.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["_key"] = key;
                    row["member"] = ConvertJsonValue(item);
                    results.Add(row);
                }
            }

            _logger.LogInformation("Redis: read {Count} members from set '{Key}'.", results.Count, key);
            return results;
        }

        // ── SORTED_SET resource ──────────────────────────────────────────────

        private async Task<List<object>> ReadSortedSetAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var key = GetRequiredKeyParam(parameters);
            var json = await ExecuteCommandAsync(host, accessToken, new object[] { "ZRANGE", key, "0", "-1", "WITHSCORES" });
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("result", out var result)
                && result.ValueKind == JsonValueKind.Array)
            {
                var items = result.EnumerateArray().ToList();
                for (int i = 0; i < items.Count - 1; i += 2)
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    row["_key"] = key;
                    row["member"] = ConvertJsonValue(items[i]);
                    row["score"] = ConvertJsonValue(items[i + 1]);
                    results.Add(row);
                }
            }

            _logger.LogInformation("Redis: read {Count} members from sorted set '{Key}'.", results.Count, key);
            return results;
        }

        // ── STREAM resource ─────────────────────────────────────────────────

        private async Task<List<object>> ReadStreamAsync(
            string host, string accessToken, Dictionary<string, object> parameters)
        {
            var key = GetRequiredKeyParam(parameters);
            var json = await ExecuteCommandAsync(host, accessToken, new object[] { "XRANGE", key, "-", "+" });
            using var doc = JsonDocument.Parse(json);

            var results = new List<object>();

            if (doc.RootElement.TryGetProperty("result", out var result)
                && result.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in result.EnumerateArray())
                {
                    if (entry.ValueKind == JsonValueKind.Array && entry.GetArrayLength() >= 2)
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        row["_key"] = key;
                        row["_id"] = ConvertJsonValue(entry[0]);

                        // Fields: [field1, value1, field2, value2, ...]
                        if (entry[1].ValueKind == JsonValueKind.Array)
                        {
                            var fields = entry[1].EnumerateArray().ToList();
                            for (int i = 0; i < fields.Count - 1; i += 2)
                            {
                                var fieldName = fields[i].GetString() ?? $"field_{i}";
                                row[fieldName] = ConvertJsonValue(fields[i + 1]);
                            }
                        }

                        results.Add(row);
                    }
                }
            }

            _logger.LogInformation("Redis: read {Count} entries from stream '{Key}'.", results.Count, key);
            return results;
        }

        // ── REST API command execution ───────────────────────────────────────

        private async Task<string> ExecuteCommandAsync(string host, string accessToken, object[] command)
        {
            var body = JsonSerializer.Serialize(command);

            using var request = new HttpRequestMessage(HttpMethod.Post, host.TrimEnd('/'))
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync();
        }

        // ── JSON value conversion ────────────────────────────────────────────

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True   => true,
                JsonValueKind.False  => false,
                JsonValueKind.Null   => string.Empty,
                _                    => value.ToString()
            };
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"host":"...","accessToken":"...","port":6379}
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
                    "Redis access token is required. Provide it via Parameters['accessToken'] or the connection string JSON.",
                    new ArgumentException("Missing 'accessToken'."),
                    "redis");
            return token;
        }

        private static string GetRequiredKeyParam(Dictionary<string, object> parameters)
        {
            var key = GetStringParam(parameters, "key");
            if (string.IsNullOrWhiteSpace(key))
                throw new ConnectorException(
                    "Redis connector requires the 'key' parameter for this resource.",
                    new ArgumentException("Missing 'key'."),
                    "redis");
            return key;
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
                    $"Redis connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "redis");
            return value;
        }
    }
}
