#nullable enable
using System;
using System.Collections.Generic;
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
    /// Upserts vectors into a Pinecone index.
    /// config.ConnectionString = JSON: {"apiKey":"...","indexName":"...","environment":"..."}
    /// config.Parameters["namespace"] = optional Pinecone namespace
    ///
    /// Records MUST contain:
    ///   "id"     (string) — unique vector ID
    ///   "values" (float[] or comma-separated string) — embedding vector
    ///   Any other fields are stored as metadata.
    /// </summary>
    public class PineconeWriter : IDestinationWriter
    {
        private readonly ILogger _logger;

        public PineconeWriter(ILogger<PineconeWriter> logger)
        {
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            _logger.LogInformation("Upserting vectors to Pinecone index.");
            try
            {
                var opts   = ParseConnectionString(config.ConnectionString);
                var ns     = GetNamespace(config);
                var client = new PineconeClient(opts.ApiKey);
                var index  = client.Index(opts.IndexName);
                var rows   = records.ToList();
                if (rows.Count == 0) return;

                var vectors = rows.Select(r => RecordToVector(r)).ToList();
                await index.UpsertAsync(new UpsertRequest { Vectors = vectors, Namespace = ns });
                _logger.LogInformation("Upserted {Count} vectors into Pinecone.", vectors.Count);
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Failed to write to Pinecone. See inner exception.", ex);
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Pinecone requires "id" and "values" fields
            var fieldSet = new HashSet<string>(fields, StringComparer.OrdinalIgnoreCase);
            return Task.FromResult(fieldSet.Contains("id") && fieldSet.Contains("values"));
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for Pinecone write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
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

        private static string? GetNamespace(DataSourceConfig config) =>
            config.Parameters.TryGetValue("namespace", out var n) ? n?.ToString() : null;

        private static Vector RecordToVector(object record)
        {
            if (record is not IDictionary<string, object> dict)
                throw new InvalidOperationException("Record must be a dictionary with 'id' and 'values' fields.");

            var id = dict.TryGetValue("id", out var idVal) ? idVal?.ToString() ?? Guid.NewGuid().ToString() : Guid.NewGuid().ToString();

            float[] values;
            if (dict.TryGetValue("values", out var rawValues))
            {
                values = rawValues switch
                {
                    float[] fa => fa,
                    IEnumerable<float> ef => ef.ToArray(),
                    string s => s.Split(',').Select(x => float.Parse(x.Trim())).ToArray(),
                    _ => Array.Empty<float>()
                };
            }
            else
            {
                values = Array.Empty<float>();
            }

            // All other fields become metadata
            var metadata = new Dictionary<string, MetadataValue>();
            foreach (var kv in dict.Where(k => k.Key != "id" && k.Key != "values"))
                metadata[kv.Key] = new MetadataValue(kv.Value?.ToString() ?? string.Empty);

            return new Vector { Id = id, Values = values, Metadata = (Pinecone.Metadata)metadata };
        }

        private class PineconeConnectionOptions
        {
            public string ApiKey { get; set; } = string.Empty;
            public string IndexName { get; set; } = string.Empty;
            public string Environment { get; set; } = string.Empty;
        }
    }
}
