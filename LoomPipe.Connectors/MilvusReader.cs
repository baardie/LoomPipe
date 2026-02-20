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
    /// Reads entities from a Milvus collection using the Milvus REST API v2.
    /// config.ConnectionString = JSON:
    ///   {"host":"localhost","port":19530,"collection":"my_collection","user":"","password":""}
    /// config.Parameters["collection"] = collection name (overrides CS)
    /// config.Parameters["limit"] = max records to fetch (default: 100)
    /// </summary>
    public class MilvusReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger _logger;

        public MilvusReader(HttpClient httpClient, ILogger<MilvusReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from Milvus collection '{Collection}'.", GetCollection(config));
            try
            {
                return await QueryAsync(config, GetLimit(config));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to read from Milvus: {ex.Message}", ex, "milvus");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for Milvus collection '{Collection}'.", GetCollection(config));
            try
            {
                var records = await QueryAsync(config, 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to discover Milvus schema: {ex.Message}", ex, "milvus");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview from Milvus collection '{Collection}'.", GetCollection(config));
            try
            {
                return await QueryAsync(config, sampleSize);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Milvus dry run failed: {ex.Message}", ex, "milvus");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private async Task<List<object>> QueryAsync(DataSourceConfig config, int limit)
        {
            var opts       = ParseConnectionString(config.ConnectionString);
            var collection = GetCollection(config);
            var baseUrl    = $"http://{opts.Host}:{opts.Port}";

            var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/v2/vectordb/entities/query");
            if (!string.IsNullOrEmpty(opts.User))
            {
                var token = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{opts.User}:{opts.Password}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);
            }

            var body = JsonSerializer.Serialize(new
            {
                collectionName = collection,
                filter         = "id >= 0",
                limit,
                outputFields   = new[] { "*" },
            });
            request.Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
                return new List<object>();

            return data.EnumerateArray().Select(elem =>
            {
                IDictionary<string, object> expando = new ExpandoObject();
                foreach (var prop in elem.EnumerateObject())
                    expando[prop.Name] = JsonElementToObject(prop.Value);
                return (object)expando;
            }).ToList();
        }

        private static object JsonElementToObject(JsonElement elem) => elem.ValueKind switch
        {
            JsonValueKind.String => elem.GetString() ?? string.Empty,
            JsonValueKind.Number => elem.TryGetInt64(out var i) ? i : elem.GetDouble(),
            JsonValueKind.True   => true,
            JsonValueKind.False  => false,
            _                    => elem.ToString()
        };

        private static MilvusConnectionOptions ParseConnectionString(string cs)
        {
            try
            {
                return JsonSerializer.Deserialize<MilvusConnectionOptions>(cs)
                       ?? throw new InvalidOperationException("Null result deserializing Milvus connection string.");
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("Milvus connection string must be JSON: {\"host\":\"...\",\"port\":19530,\"collection\":\"...\"}");
            }
        }

        private static string GetCollection(DataSourceConfig config)
        {
            if (config.Parameters.TryGetValue("collection", out var c) && !string.IsNullOrEmpty(c?.ToString()))
                return c.ToString()!;
            var opts = ParseConnectionString(config.ConnectionString);
            return opts.Collection;
        }

        private static int GetLimit(DataSourceConfig config) =>
            config.Parameters.TryGetValue("limit", out var v) && int.TryParse(v?.ToString(), out var i) ? i : 100;

        private class MilvusConnectionOptions
        {
            public string Host { get; set; } = "localhost";
            public int Port { get; set; } = 19530;
            public string Collection { get; set; } = string.Empty;
            public string User { get; set; } = string.Empty;
            public string Password { get; set; } = string.Empty;
        }
    }
}
