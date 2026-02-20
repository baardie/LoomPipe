#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Pinecone;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads vectors from a Pinecone index.
    /// config.ConnectionString = JSON: {"apiKey":"...","indexName":"...","environment":"..."}
    /// config.Parameters["topK"] = number of vectors to retrieve (default: 100)
    /// config.Parameters["namespace"] = optional Pinecone namespace
    ///
    /// Records are returned as ExpandoObjects with fields: id, score, [metadata fields]
    /// </summary>
    public class PineconeReader : ISourceReader
    {
        private readonly ILogger _logger;

        public PineconeReader(ILogger<PineconeReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from Pinecone index.");
            try
            {
                var opts      = ParseConnectionString(config.ConnectionString);
                var topK      = GetTopK(config);
                var ns        = GetNamespace(config);

                var client    = new PineconeClient(opts.ApiKey);
                var index     = client.Index(opts.IndexName);
                var dimension = await GetDimensionAsync(index);
                var zeroVec   = new float[dimension];

                var response = await index.QueryAsync(new QueryRequest
                {
                    Vector          = zeroVec,
                    TopK            = (uint)topK,
                    Namespace       = ns,
                    IncludeMetadata = true,
                });

                return (response.Matches ?? Enumerable.Empty<ScoredVector>())
                    .Select(m => MatchToExpando(m))
                    .ToList<object>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to read from Pinecone: {ex.Message}", ex, "pinecone");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema from Pinecone index.");
            try
            {
                var records = await ReadAsync(config);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to discover Pinecone schema: {ex.Message}", ex, "pinecone");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview from Pinecone.");
            var records = await ReadAsync(config);
            return records.Take(sampleSize);
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static PineconeConnectionOptions ParseConnectionString(string cs)
        {
            try
            {
                return JsonSerializer.Deserialize<PineconeConnectionOptions>(cs)
                       ?? throw new InvalidOperationException("Null result deserializing Pinecone connection string.");
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("Pinecone connection string must be JSON: {\"apiKey\":\"...\",\"indexName\":\"...\",\"environment\":\"...\"}");
            }
        }

        private static int GetTopK(DataSourceConfig config) =>
            config.Parameters.TryGetValue("topK", out var v) && int.TryParse(v?.ToString(), out var i) ? i : 100;

        private static string? GetNamespace(DataSourceConfig config) =>
            config.Parameters.TryGetValue("namespace", out var n) ? n?.ToString() : null;

        private static async Task<int> GetDimensionAsync(IndexClient index)
        {
            var stats = await index.DescribeIndexStatsAsync(new DescribeIndexStatsRequest());
            return (int)(stats.Dimension ?? 1536);
        }

        private static object MatchToExpando(ScoredVector m)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            expando["id"]    = m.Id;
            expando["score"] = m.Score;
            if (m.Metadata != null)
                foreach (var kv in m.Metadata)
                    expando[kv.Key] = kv.Value?.ToString() ?? string.Empty;
            return expando;
        }

        private class PineconeConnectionOptions
        {
            public string ApiKey { get; set; } = string.Empty;
            public string IndexName { get; set; } = string.Empty;
            public string Environment { get; set; } = string.Empty;
        }
    }
}
