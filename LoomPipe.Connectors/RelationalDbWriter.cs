#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
    /// Writes rows to relational databases (SQL Server, PostgreSQL, MySQL, Oracle) via ADO.NET.
    /// Uses parameterized INSERT statements to prevent SQL injection.
    /// config.ConnectionString = provider connection string (resolved from ConnectionProfile).
    /// config.Parameters["table"] = target table name.
    /// </summary>
    public class RelationalDbWriter : IDestinationWriter
    {
        private readonly string _provider;
        private readonly ILogger _logger;

        public RelationalDbWriter(string provider, ILogger<RelationalDbWriter> logger)
        {
            _provider = provider.ToLowerInvariant();
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var table = GetTable(config);
            _logger.LogInformation("Writing to {Provider} table '{Table}'.", _provider, table);
            try
            {
                var rows = records.ToList();
                if (rows.Count == 0) return;

                await using var conn = CreateConnection(config.ConnectionString);
                await conn.OpenAsync();

                foreach (var record in rows)
                {
                    if (record is not IDictionary<string, object> dict) continue;
                    var cols = dict.Keys.ToList();
                    var paramNames = cols.Select((_, i) => $"@p{i}").ToList();
                    var sql = $"INSERT INTO {table} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", paramNames)})";

                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = sql;
                    for (int i = 0; i < cols.Count; i++)
                    {
                        var param = cmd.CreateParameter();
                        param.ParameterName = $"@p{i}";
                        param.Value = dict[cols[i]] ?? DBNull.Value;
                        cmd.Parameters.Add(param);
                    }
                    await cmd.ExecuteNonQueryAsync();
                }

                _logger.LogInformation("Wrote {Count} rows to {Provider} table '{Table}'.", rows.Count, _provider, table);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to {_provider}. See inner exception.", ex);
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            var table = GetTable(config);
            _logger.LogInformation("Validating schema for {Provider} table '{Table}'.", _provider, table);
            try
            {
                await using var conn = CreateConnection(config.ConnectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'";
                await using var reader = await cmd.ExecuteReaderAsync();
                var dbCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                while (await reader.ReadAsync())
                    dbCols.Add(reader.GetString(0));
                return fields.All(f => dbCols.Contains(f));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Schema validation failed for {_provider}. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for {Provider} write (no data written).", _provider);
            var sample = records.Take(sampleSize).ToList();
            if (sample.Count > 0 && sample[0] is IDictionary<string, object> dict)
                await ValidateSchemaAsync(config, dict.Keys);
            return sample;
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
    }
}
