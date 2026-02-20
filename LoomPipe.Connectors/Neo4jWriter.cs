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
using Neo4j.Driver;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Writes/merges nodes into a Neo4j graph database.
    /// config.ConnectionString = JSON: {"uri":"bolt://host:7687","user":"neo4j","password":"..."}
    /// config.Parameters["label"] = node label (e.g. "Person")
    /// config.Parameters["idField"] = field name used as the unique key for MERGE (default: "id")
    /// </summary>
    public class Neo4jWriter : IDestinationWriter
    {
        private readonly ILogger _logger;

        public Neo4jWriter(ILogger<Neo4jWriter> logger)
        {
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var label   = GetLabel(config);
            var idField = GetIdField(config);
            _logger.LogInformation("Writing to Neo4j label '{Label}' with id field '{IdField}'.", label, idField);
            try
            {
                var opts   = ParseConnectionString(config.ConnectionString);
                var driver = GraphDatabase.Driver(opts.Uri, AuthTokens.Basic(opts.User, opts.Password));
                await using var session = driver.AsyncSession();

                foreach (var record in records)
                {
                    if (record is not IDictionary<string, object> dict) continue;

                    var idValue = dict.TryGetValue(idField, out var v) ? v : null;
                    var props = dict.ToDictionary(k => k.Key, k => k.Value);

                    var cypher = $"MERGE (n:{label} {{{idField}: $idVal}}) SET n += $props";
                    await session.RunAsync(cypher, new { idVal = idValue, props });
                }

                await driver.DisposeAsync();
                _logger.LogInformation("Finished writing to Neo4j label '{Label}'.", label);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to Neo4j: {ex.Message}", ex, "neo4j");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Neo4j is schemaless — always valid
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for Neo4j write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static Neo4jConnectionOptions ParseConnectionString(string cs)
        {
            try
            {
                return JsonSerializer.Deserialize<Neo4jConnectionOptions>(cs)
                       ?? throw new InvalidOperationException("Null result deserializing Neo4j connection string.");
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("Neo4j connection string must be JSON: {\"uri\":\"bolt://...\",\"user\":\"...\",\"password\":\"...\"}");
            }
        }

        private static string GetLabel(DataSourceConfig config) =>
            config.Parameters.TryGetValue("label", out var l) ? l?.ToString() ?? "Node" : "Node";

        private static string GetIdField(DataSourceConfig config) =>
            config.Parameters.TryGetValue("idField", out var f) ? f?.ToString() ?? "id" : "id";

        private class Neo4jConnectionOptions
        {
            public string Uri { get; set; } = string.Empty;
            public string User { get; set; } = string.Empty;
            public string Password { get; set; } = string.Empty;
        }
    }
}
