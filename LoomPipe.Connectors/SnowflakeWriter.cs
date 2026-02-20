#nullable enable
using System;
using System.Collections.Generic;
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
    /// Writes rows to a Snowflake table using parameterized INSERT statements.
    /// config.ConnectionString = Snowflake ADO.NET connection string
    /// config.Parameters["table"] = target table name
    /// </summary>
    public class SnowflakeWriter : IDestinationWriter
    {
        private readonly ILogger _logger;

        public SnowflakeWriter(ILogger<SnowflakeWriter> logger)
        {
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var table = GetTable(config);
            _logger.LogInformation("Writing to Snowflake table '{Table}'.", table);
            try
            {
                var rows = records.ToList();
                if (rows.Count == 0) return;

                await using var conn = new SnowflakeDbConnection();
                conn.ConnectionString = config.ConnectionString;
                await conn.OpenAsync();

                foreach (var record in rows)
                {
                    if (record is not IDictionary<string, object> dict) continue;
                    var cols = dict.Keys.ToList();
                    // Snowflake uses ? positional parameters
                    var placeholders = string.Join(", ", cols.Select(_ => "?"));
                    var sql = $"INSERT INTO {table} ({string.Join(", ", cols)}) VALUES ({placeholders})";

                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = sql;
                    foreach (var col in cols)
                    {
                        var param = cmd.CreateParameter();
                        param.Value = dict[col] ?? DBNull.Value;
                        cmd.Parameters.Add(param);
                    }
                    await cmd.ExecuteNonQueryAsync();
                }

                _logger.LogInformation("Wrote {Count} rows to Snowflake table '{Table}'.", rows.Count, table);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to Snowflake: {ex.Message}", ex, "snowflake");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Schema validation for Snowflake deferred to INFORMATION_SCHEMA query if needed
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for Snowflake write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        private static string GetTable(DataSourceConfig config) =>
            config.Parameters.TryGetValue("table", out var t) ? t?.ToString() ?? "data" : "data";
    }
}
