#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads rows from a ClickHouse database via the ClickHouse.Client ADO.NET driver.
    /// ConnectionString JSON: {"host":"...","port":8123,"database":"...","username":"...","password":"..."}
    /// Parameters: host, port, database, table, username, password, query (optional SQL override).
    /// </summary>
    public class ClickHouseReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<ClickHouseReader> _logger;

        public ClickHouseReader(HttpClient httpClient, ILogger<ClickHouseReader> logger)
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
            var database = GetStringParam(parameters, "database") ?? "default";
            _logger.LogInformation("Reading from ClickHouse database '{Database}', table '{Table}'.", database, table);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();

                var customQuery = GetStringParam(parameters, "query");
                var sql = !string.IsNullOrWhiteSpace(customQuery)
                    ? customQuery
                    : $"SELECT * FROM {table}";

                if (!string.IsNullOrWhiteSpace(watermarkField) && !string.IsNullOrWhiteSpace(watermarkValue))
                {
                    sql = !string.IsNullOrWhiteSpace(customQuery)
                        ? $"SELECT * FROM ({customQuery}) _q WHERE {watermarkField} > '{watermarkValue}'"
                        : $"SELECT * FROM {table} WHERE {watermarkField} > '{watermarkValue}'";
                }

                return await ExecuteQueryAsync(conn, sql);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from ClickHouse database '{Database}', table '{Table}'.", database, table);
                throw new ConnectorException($"Failed to read from ClickHouse: {ex.Message}", ex, "clickhouse");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            var database = GetStringParam(parameters, "database") ?? "default";
            _logger.LogInformation("Discovering schema for ClickHouse database '{Database}', table '{Table}'.", database, table);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {table} LIMIT 1";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

                var columns = new List<string>();
                for (int i = 0; i < reader.FieldCount; i++)
                    columns.Add(reader.GetName(i));
                return columns;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for ClickHouse table '{Table}'.", table);
                throw new ConnectorException($"Failed to discover ClickHouse schema: {ex.Message}", ex, "clickhouse");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            var database = GetStringParam(parameters, "database") ?? "default";
            _logger.LogInformation("Dry run preview from ClickHouse database '{Database}', table '{Table}'.", database, table);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, $"SELECT * FROM {table} LIMIT {sampleSize}");
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for ClickHouse table '{Table}'.", table);
                throw new ConnectorException($"ClickHouse dry run failed: {ex.Message}", ex, "clickhouse");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var database = GetStringParam(parameters, "database") ?? "default";
            _logger.LogInformation("Listing tables in ClickHouse database '{Database}'.", database);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SHOW TABLES FROM {database}";
                await using var reader = await cmd.ExecuteReaderAsync();

                var tables = new List<string>();
                while (await reader.ReadAsync())
                {
                    tables.Add(reader.GetString(0));
                }
                return tables;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list tables in ClickHouse database '{Database}'.", database);
                throw new ConnectorException($"Failed to list ClickHouse tables: {ex.Message}", ex, "clickhouse");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static ClickHouseConnection CreateConnection(Dictionary<string, object> parameters)
        {
            var host = GetStringParam(parameters, "host") ?? "localhost";
            var portStr = GetStringParam(parameters, "port");
            var port = !string.IsNullOrEmpty(portStr) && int.TryParse(portStr, out var p) ? p : 8123;
            var database = GetStringParam(parameters, "database") ?? "default";
            var username = GetStringParam(parameters, "username") ?? "default";
            var password = GetStringParam(parameters, "password") ?? "";

            var connString = $"Host={host};Port={port};Database={database};Username={username};Password={password}";
            return new ClickHouseConnection(connString);
        }

        private static async Task<List<object>> ExecuteQueryAsync(DbConnection conn, string sql)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync();
            var results = new List<object>();
            while (await reader.ReadAsync())
            {
                IDictionary<string, object> expando = new ExpandoObject();
                for (int i = 0; i < reader.FieldCount; i++)
                    expando[reader.GetName(i)] = reader.IsDBNull(i) ? string.Empty : reader.GetValue(i);
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
