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
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads rows from relational databases (SQL Server, PostgreSQL, MySQL, Oracle) via ADO.NET.
    /// config.ConnectionString = provider connection string (resolved from ConnectionProfile).
    /// config.Parameters["table"] = table name to SELECT from.
    /// </summary>
    public class RelationalDbReader : ISourceReader
    {
        private readonly string _provider;
        private readonly ILogger _logger;

        public RelationalDbReader(string provider, ILogger<RelationalDbReader> logger)
        {
            _provider = provider.ToLowerInvariant();
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from {Provider} table '{Table}'.", _provider, GetTable(config));
            try
            {
                await using var conn = CreateConnection(config.ConnectionString);
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, $"SELECT * FROM {GetTable(config)}");
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to read from {_provider}. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for {Provider} table '{Table}'.", _provider, GetTable(config));
            try
            {
                await using var conn = CreateConnection(config.ConnectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = LimitedQuery($"SELECT * FROM {GetTable(config)}", 1);
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = await reader.GetSchemaTableAsync();
                if (schema == null) return Array.Empty<string>();
                return schema.Rows.Cast<DataRow>()
                    .Select(r => r["ColumnName"]?.ToString() ?? string.Empty)
                    .Where(n => !string.IsNullOrEmpty(n));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to discover schema for {_provider}. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview from {Provider} table '{Table}'.", _provider, GetTable(config));
            try
            {
                await using var conn = CreateConnection(config.ConnectionString);
                await conn.OpenAsync();
                return await ExecuteQueryAsync(conn, LimitedQuery($"SELECT * FROM {GetTable(config)}", sampleSize));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Dry run failed for {_provider}. See inner exception.", ex);
            }
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private DbConnection CreateConnection(string cs) => _provider switch
        {
            "sqlserver"  => new SqlConnection(cs),
            "postgresql" => new NpgsqlConnection(cs),
            "mysql"      => new MySqlConnection(cs),
            "oracle"     => new OracleConnection(cs),
            _            => throw new NotSupportedException($"Provider '{_provider}' is not supported.")
        };

        private static string GetTable(DataSourceConfig config) =>
            config.Parameters.TryGetValue("table", out var t) ? t?.ToString() ?? "data" : "data";

        private string LimitedQuery(string baseSql, int limit) => _provider switch
        {
            "sqlserver" => $"SELECT TOP {limit} * FROM ({baseSql}) _q",
            "oracle"    => $"SELECT * FROM ({baseSql}) WHERE ROWNUM <= {limit}",
            _           => $"{baseSql} LIMIT {limit}",
        };

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
