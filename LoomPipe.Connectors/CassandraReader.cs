#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Cassandra;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads rows from an Apache Cassandra cluster.
    /// ConnectionString JSON: {"host":"...","port":9042,"keyspace":"...","username":"...","password":"..."}
    /// Parameters: host, port, keyspace, table, username, password, query (optional CQL override).
    /// </summary>
    public class CassandraReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<CassandraReader> _logger;

        public CassandraReader(HttpClient httpClient, ILogger<CassandraReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            var keyspace = GetStringParam(parameters, "keyspace") ?? "default";
            _logger.LogInformation("Reading from Cassandra keyspace '{Keyspace}', table '{Table}'.", keyspace, table);

            try
            {
                using var cluster = BuildCluster(parameters);
                using var session = cluster.Connect(keyspace);

                var customQuery = GetStringParam(parameters, "query");
                var cql = !string.IsNullOrWhiteSpace(customQuery)
                    ? customQuery
                    : $"SELECT * FROM {table}";

                if (!string.IsNullOrWhiteSpace(watermarkField) && !string.IsNullOrWhiteSpace(watermarkValue))
                {
                    cql = !string.IsNullOrWhiteSpace(customQuery)
                        ? $"SELECT * FROM ({customQuery}) WHERE {watermarkField} > '{watermarkValue}' ALLOW FILTERING"
                        : $"SELECT * FROM {table} WHERE {watermarkField} > '{watermarkValue}' ALLOW FILTERING";
                }

                var rowSet = await Task.Run(() => session.Execute(cql));
                return RowSetToExpandoList(rowSet);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Cassandra keyspace '{Keyspace}', table '{Table}'.", keyspace, table);
                throw new ConnectorException($"Failed to read from Cassandra: {ex.Message}", ex, "cassandra");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            var keyspace = GetStringParam(parameters, "keyspace") ?? "default";
            _logger.LogInformation("Discovering schema for Cassandra keyspace '{Keyspace}', table '{Table}'.", keyspace, table);

            try
            {
                using var cluster = BuildCluster(parameters);
                using var session = cluster.Connect(keyspace);

                var rowSet = await Task.Run(() => session.Execute($"SELECT * FROM {table} LIMIT 1"));
                return rowSet.Columns.Select(c => c.Name);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Cassandra table '{Table}'.", table);
                throw new ConnectorException($"Failed to discover Cassandra schema: {ex.Message}", ex, "cassandra");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            var keyspace = GetStringParam(parameters, "keyspace") ?? "default";
            _logger.LogInformation("Dry run preview from Cassandra keyspace '{Keyspace}', table '{Table}'.", keyspace, table);

            try
            {
                using var cluster = BuildCluster(parameters);
                using var session = cluster.Connect(keyspace);

                var rowSet = await Task.Run(() => session.Execute($"SELECT * FROM {table} LIMIT {sampleSize}"));
                return RowSetToExpandoList(rowSet);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for Cassandra table '{Table}'.", table);
                throw new ConnectorException($"Cassandra dry run failed: {ex.Message}", ex, "cassandra");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var keyspace = GetStringParam(parameters, "keyspace") ?? "default";
            _logger.LogInformation("Listing tables in Cassandra keyspace '{Keyspace}'.", keyspace);

            try
            {
                using var cluster = BuildCluster(parameters);
                using var session = cluster.Connect(keyspace);

                var rowSet = await Task.Run(() =>
                    session.Execute($"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"));

                return rowSet.Select(row => row.GetValue<string>("table_name")).ToList();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list tables in Cassandra keyspace '{Keyspace}'.", keyspace);
                throw new ConnectorException($"Failed to list Cassandra tables: {ex.Message}", ex, "cassandra");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static Cluster BuildCluster(Dictionary<string, object> parameters)
        {
            var host = GetStringParam(parameters, "host") ?? "localhost";
            var portStr = GetStringParam(parameters, "port");
            var port = !string.IsNullOrEmpty(portStr) && int.TryParse(portStr, out var p) ? p : 9042;
            var username = GetStringParam(parameters, "username");
            var password = GetStringParam(parameters, "password");

            var builder = Cluster.Builder()
                .AddContactPoint(host)
                .WithPort(port);

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
                builder = builder.WithCredentials(username, password);

            return builder.Build();
        }

        private static List<object> RowSetToExpandoList(RowSet rowSet)
        {
            var columns = rowSet.Columns;
            var results = new List<object>();

            foreach (var row in rowSet)
            {
                IDictionary<string, object> expando = new ExpandoObject();
                for (int i = 0; i < columns.Length; i++)
                {
                    var value = row.GetValue<object>(i);
                    expando[columns[i].Name] = value ?? string.Empty;
                }
                results.Add(expando);
            }

            return results;
        }

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
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

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }
    }
}
