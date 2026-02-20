#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Writes rows into a Google BigQuery table using streaming inserts.
    /// config.ConnectionString = JSON:
    ///   {"projectId":"my-project","dataset":"my_dataset","serviceAccountJson":"..."}
    /// config.Parameters["table"] = target table name
    /// </summary>
    public class BigQueryWriter : IDestinationWriter
    {
        private readonly ILogger _logger;

        public BigQueryWriter(ILogger<BigQueryWriter> logger)
        {
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var table = GetTable(config);
            _logger.LogInformation("Writing to BigQuery table '{Table}'.", table);
            try
            {
                var rows = records.ToList();
                if (rows.Count == 0) return;

                var (client, dataset) = CreateClient(config);
                var insertRows = rows
                    .Select(r => RecordToInsertRow(r))
                    .ToList();

                await client.InsertRowsAsync(dataset, table, insertRows);
                _logger.LogInformation("Streamed {Count} rows into BigQuery table '{Table}'.", rows.Count, table);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to BigQuery: {ex.Message}", ex, "bigquery");
            }
        }

        public async Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            _logger.LogInformation("Validating BigQuery schema for table '{Table}'.", GetTable(config));
            try
            {
                var (client, dataset) = CreateClient(config);
                var bqTable = await client.GetTableAsync(dataset, GetTable(config));
                var tableFields = new HashSet<string>(bqTable.Schema.Fields.Select(f => f.Name), StringComparer.OrdinalIgnoreCase);
                return fields.All(f => tableFields.Contains(f));
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"BigQuery schema validation failed: {ex.Message}", ex, "bigquery");
            }
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for BigQuery write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static (BigQueryClient client, string dataset) CreateClient(DataSourceConfig config)
        {
            var opts = ParseConnectionString(config.ConnectionString);
            var credential = GoogleCredential.FromJson(opts.ServiceAccountJson)
                .CreateScoped("https://www.googleapis.com/auth/bigquery");
            var client = BigQueryClient.Create(opts.ProjectId, credential);
            return (client, opts.Dataset);
        }

        private static BigQueryConnectionOptions ParseConnectionString(string cs)
        {
            try
            {
                return JsonSerializer.Deserialize<BigQueryConnectionOptions>(cs)
                       ?? throw new InvalidOperationException("Null result deserializing BigQuery connection string.");
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("BigQuery connection string must be JSON.");
            }
        }

        private static string GetTable(DataSourceConfig config) =>
            config.Parameters.TryGetValue("table", out var t) ? t?.ToString() ?? "data" : "data";

        private static BigQueryInsertRow RecordToInsertRow(object record)
        {
            var row = new BigQueryInsertRow();
            if (record is IDictionary<string, object> dict)
                foreach (var kv in dict)
                    row[kv.Key] = kv.Value;
            return row;
        }

        private class BigQueryConnectionOptions
        {
            public string ProjectId { get; set; } = string.Empty;
            public string Dataset { get; set; } = string.Empty;
            public string ServiceAccountJson { get; set; } = string.Empty;
        }
    }
}
