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
    /// Reads nodes from a Neo4j graph database.
    /// config.ConnectionString = JSON: {"uri":"bolt://host:7687","user":"neo4j","password":"..."}
    /// config.Parameters["label"] = node label to MATCH (e.g. "Person")
    /// config.Parameters["query"] = optional custom Cypher query (overrides label)
    /// </summary>
    public class Neo4jReader : ISourceReader
    {
        private readonly ILogger _logger;

        public Neo4jReader(ILogger<Neo4jReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from Neo4j (label='{Label}').", GetLabel(config));
            try
            {
                var (driver, cypher) = CreateDriverAndQuery(config, limit: null);
                await using var session = driver.AsyncSession();
                var result = await session.RunAsync(cypher);
                var records = await result.ToListAsync();
                await driver.DisposeAsync();
                return records.Select(r => RecordToExpando(r)).ToList<object>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Failed to read from Neo4j. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering Neo4j schema for label '{Label}'.", GetLabel(config));
            try
            {
                var (driver, cypher) = CreateDriverAndQuery(config, limit: 1);
                await using var session = driver.AsyncSession();
                var result = await session.RunAsync(cypher);
                var allRecords = await result.ToListAsync();
                await driver.DisposeAsync();
                if (allRecords.Count == 0) return Array.Empty<string>();
                var node = allRecords[0]["n"].As<INode>();
                return node.Properties.Keys;
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Failed to discover Neo4j schema. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview from Neo4j (label='{Label}').", GetLabel(config));
            try
            {
                var (driver, cypher) = CreateDriverAndQuery(config, limit: sampleSize);
                await using var session = driver.AsyncSession();
                var result = await session.RunAsync(cypher);
                var records = await result.ToListAsync();
                await driver.DisposeAsync();
                return records.Select(r => RecordToExpando(r)).ToList<object>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Neo4j dry run failed. See inner exception.", ex);
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static (IDriver driver, string cypher) CreateDriverAndQuery(DataSourceConfig config, int? limit)
        {
            var opts = ParseConnectionString(config.ConnectionString);
            var driver = GraphDatabase.Driver(opts.Uri, AuthTokens.Basic(opts.User, opts.Password));

            string cypher;
            if (config.Parameters.TryGetValue("query", out var customQuery) && !string.IsNullOrEmpty(customQuery?.ToString()))
            {
                cypher = customQuery.ToString()!;
            }
            else
            {
                var label = GetLabel(config);
                cypher = limit.HasValue
                    ? $"MATCH (n:{label}) RETURN n LIMIT {limit}"
                    : $"MATCH (n:{label}) RETURN n";
            }

            return (driver, cypher);
        }

        private static Neo4jConnectionOptions ParseConnectionString(string cs)
        {
            try
            {
                return JsonSerializer.Deserialize<Neo4jConnectionOptions>(cs)
                       ?? throw new InvalidOperationException("Null result deserialing Neo4j connection string.");
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("Neo4j connection string must be JSON: {\"uri\":\"bolt://...\",\"user\":\"...\",\"password\":\"...\"}");
            }
        }

        private static string GetLabel(DataSourceConfig config) =>
            config.Parameters.TryGetValue("label", out var l) ? l?.ToString() ?? "Node" : "Node";

        private static object RecordToExpando(IRecord record)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            if (record["n"] is INode node)
                foreach (var prop in node.Properties)
                    expando[prop.Key] = prop.Value;
            return expando;
        }

        private class Neo4jConnectionOptions
        {
            public string Uri { get; set; } = string.Empty;
            public string User { get; set; } = string.Empty;
            public string Password { get; set; } = string.Empty;
        }
    }
}
