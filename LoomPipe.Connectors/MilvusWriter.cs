#nullable enable
using System;
using System.Collections.Generic;
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
    /// Inserts entities into a Milvus collection using the Milvus REST API v2.
    /// config.ConnectionString = JSON:
    ///   {"host":"localhost","port":19530,"collection":"my_collection","user":"","password":""}
    /// config.Parameters["collection"] = collection name (overrides CS)
    /// </summary>
    public class MilvusWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger _logger;

        public MilvusWriter(HttpClient httpClient, ILogger<MilvusWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var collection = GetCollection(config);
            _logger.LogInformation("Inserting into Milvus collection '{Collection}'.", collection);
            try
            {
                var rows = records.ToList();
                if (rows.Count == 0) return;

                var opts    = ParseConnectionString(config.ConnectionString);
                var baseUrl = $"http://{opts.Host}:{opts.Port}";

                var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/v2/vectordb/entities/insert");
                if (!string.IsNullOrEmpty(opts.User))
                {
                    var token = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{opts.User}:{opts.Password}"));
                    request.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);
                }

                var body = JsonSerializer.Serialize(new
                {
                    collectionName = collection,
                    data           = rows,
                });
                request.Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

                var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();
                _logger.LogInformation("Inserted {Count} entities into Milvus collection '{Collection}'.", rows.Count, collection);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to Milvus: {ex.Message}", ex, "milvus");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Schema validation for Milvus would require describing the collection — return true for now
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for Milvus write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Helpers ──────────────────────────────────────────────────────────

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
