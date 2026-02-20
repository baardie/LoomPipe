#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads rows from a Snowflake table or view.
    /// config.ConnectionString = Snowflake ADO.NET connection string
    ///   (account=...;user=...;password=...;warehouse=...;db=...;schema=...)
    /// config.Parameters["table"] = table name to SELECT from
    /// </summary>
    public class SnowflakeReader : ISourceReader
    {
        private readonly ILogger _logger;

        public SnowflakeReader(ILogger<SnowflakeReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from Snowflake table '{Table}'.", GetTable(config));
            try
            {
                await using var conn = new SnowflakeDbConnection();
                conn.ConnectionString = config.ConnectionString;
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, $"SELECT * FROM {GetTable(config)}");
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to read from Snowflake: {ex.Message}", ex, "snowflake");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for Snowflake table '{Table}'.", GetTable(config));
            try
            {
                await using var conn = new SnowflakeDbConnection();
                conn.ConnectionString = config.ConnectionString;
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {GetTable(config)} LIMIT 1";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = reader.GetSchemaTable();
                if (schema == null) return Array.Empty<string>();
                return schema.Rows.Cast<DataRow>()
                    .Select(r => r["ColumnName"]?.ToString() ?? string.Empty)
                    .Where(n => !string.IsNullOrEmpty(n));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to discover Snowflake schema: {ex.Message}", ex, "snowflake");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run from Snowflake table '{Table}'.", GetTable(config));
            try
            {
                await using var conn = new SnowflakeDbConnection();
                conn.ConnectionString = config.ConnectionString;
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, $"SELECT * FROM {GetTable(config)} LIMIT {sampleSize}");
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Snowflake dry run failed: {ex.Message}", ex, "snowflake");
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static string GetTable(DataSourceConfig config) =>
            config.Parameters.TryGetValue("table", out var t) ? t?.ToString() ?? "data" : "data";

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
    }
}
