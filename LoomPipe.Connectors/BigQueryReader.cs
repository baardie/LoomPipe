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
    /// Reads rows from a Google BigQuery table.
    /// config.ConnectionString = JSON:
    ///   {"projectId":"my-project","dataset":"my_dataset","serviceAccountJson":"..."}
    /// config.Parameters["table"] = table name within the dataset
    /// </summary>
    public class BigQueryReader : ISourceReader
    {
        private readonly ILogger _logger;

        public BigQueryReader(ILogger<BigQueryReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config, string? watermarkField = null, string? watermarkValue = null)
        {
            _logger.LogInformation("Reading from BigQuery table '{Table}'.", GetTable(config));
            try
            {
                var (client, dataset) = CreateClient(config);
                var table = GetTable(config);
                var sql = $"SELECT * FROM `{dataset}.{table}`";
                var results = await client.ExecuteQueryAsync(sql, parameters: null);
                return ResultsToExpandoList(results);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to read from BigQuery: {ex.Message}", ex, "bigquery");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering BigQuery schema for table '{Table}'.", GetTable(config));
            try
            {
                var (client, dataset) = CreateClient(config);
                var table = GetTable(config);
                var bqTable = await client.GetTableAsync(dataset, table);
                return bqTable.Schema.Fields.Select(f => f.Name);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to discover BigQuery schema: {ex.Message}", ex, "bigquery");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run from BigQuery table '{Table}'.", GetTable(config));
            try
            {
                var (client, dataset) = CreateClient(config);
                var table = GetTable(config);
                var sql = $"SELECT * FROM `{dataset}.{table}` LIMIT {sampleSize}";
                var results = await client.ExecuteQueryAsync(sql, parameters: null);
                return ResultsToExpandoList(results);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"BigQuery dry run failed: {ex.Message}", ex, "bigquery");
            }
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
                throw new InvalidOperationException("BigQuery connection string must be JSON: {\"projectId\":\"...\",\"dataset\":\"...\",\"serviceAccountJson\":\"...\"}");
            }
        }

        private static string GetTable(DataSourceConfig config) =>
            config.Parameters.TryGetValue("table", out var t) ? t?.ToString() ?? "data" : "data";

        private static List<object> ResultsToExpandoList(BigQueryResults results)
        {
            var list = new List<object>();
            foreach (var row in results)
            {
                IDictionary<string, object> expando = new ExpandoObject();
                foreach (var field in results.Schema.Fields)
                    expando[field.Name] = row[field.Name] ?? string.Empty;
                list.Add(expando);
            }
            return list;
        }

        private class BigQueryConnectionOptions
        {
            public string ProjectId { get; set; } = string.Empty;
            public string Dataset { get; set; } = string.Empty;
            public string ServiceAccountJson { get; set; } = string.Empty;
        }
    }
}
