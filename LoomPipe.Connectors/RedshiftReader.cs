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
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads rows from Amazon Redshift via the PostgreSQL wire protocol (Npgsql).
    /// ConnectionString JSON: {"host":"...","port":5439,"database":"...","username":"...","password":"..."}
    /// Parameters: host, port, database, username, password, table, query (optional SQL override).
    /// </summary>
    public class RedshiftReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<RedshiftReader> _logger;

        public RedshiftReader(HttpClient httpClient, ILogger<RedshiftReader> logger)
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
            _logger.LogInformation("Reading from Redshift table '{Table}'.", table);

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
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = !string.IsNullOrWhiteSpace(customQuery)
                        ? $"SELECT * FROM ({customQuery}) _q WHERE {watermarkField} > @wm"
                        : $"SELECT * FROM {table} WHERE {watermarkField} > @wm";
                    var p = cmd.CreateParameter();
                    p.ParameterName = "@wm";
                    p.Value = watermarkValue;
                    cmd.Parameters.Add(p);
                    return await ReadCommandAsync(cmd);
                }

                return await ExecuteQueryAsync(conn, sql);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Redshift table '{Table}'.", table);
                throw new ConnectorException($"Failed to read from Redshift: {ex.Message}", ex, "redshift");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            _logger.LogInformation("Discovering schema for Redshift table '{Table}'.", table);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = @tableName ORDER BY ordinal_position";
                var p = cmd.CreateParameter();
                p.ParameterName = "@tableName";
                p.Value = table;
                cmd.Parameters.Add(p);
                await using var reader = await cmd.ExecuteReaderAsync();

                var columns = new List<string>();
                while (await reader.ReadAsync())
                {
                    var colName = reader.GetString(0);
                    if (!string.IsNullOrEmpty(colName))
                        columns.Add(colName);
                }
                return columns;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Redshift table '{Table}'.", table);
                throw new ConnectorException($"Failed to discover Redshift schema: {ex.Message}", ex, "redshift");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var table = GetStringParam(parameters, "table") ?? "data";
            _logger.LogInformation("Dry run preview from Redshift table '{Table}'.", table);

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, $"SELECT * FROM {table} LIMIT {sampleSize}");
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for Redshift table '{Table}'.", table);
                throw new ConnectorException($"Redshift dry run failed: {ex.Message}", ex, "redshift");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            _logger.LogInformation("Listing tables in Redshift.");

            try
            {
                await using var conn = CreateConnection(parameters);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename";
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
                _logger.LogError(ex, "Failed to list tables in Redshift.");
                throw new ConnectorException($"Failed to list Redshift tables: {ex.Message}", ex, "redshift");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static NpgsqlConnection CreateConnection(Dictionary<string, object> parameters)
        {
            var host = GetStringParam(parameters, "host") ?? "localhost";
            var portStr = GetStringParam(parameters, "port");
            var port = !string.IsNullOrEmpty(portStr) && int.TryParse(portStr, out var p) ? p : 5439;
            var database = GetStringParam(parameters, "database") ?? "dev";
            var username = GetStringParam(parameters, "username") ?? "";
            var password = GetStringParam(parameters, "password") ?? "";

            var connString = $"Host={host};Port={port};Database={database};Username={username};Password={password}";
            return new NpgsqlConnection(connString);
        }

        private static async Task<List<object>> ExecuteQueryAsync(DbConnection conn, string sql)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return await ReadCommandAsync(cmd);
        }

        private static async Task<List<object>> ReadCommandAsync(DbCommand cmd)
        {
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
