#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads items from Amazon DynamoDB using the AWS SDK.
    ///
    /// Parameters:
    ///   accessKeyId      — AWS access key ID
    ///   secretAccessKey  — AWS secret access key
    ///   region           — AWS region (default "us-east-1")
    ///   tableName        — DynamoDB table name
    ///   filterExpression — Optional DynamoDB filter expression
    /// </summary>
    public class DynamoDbReader : ISourceReader
    {
        private readonly HttpClient _httpClient; // unused but kept for factory consistency
        private readonly ILogger<DynamoDbReader> _logger;

        public DynamoDbReader(HttpClient httpClient, ILogger<DynamoDbReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var parameters = MergeConnectionString(config);
            var tableName  = GetRequiredParam(parameters, "tableName");

            _logger.LogInformation("DynamoDB: reading table '{Table}'.", tableName);

            try
            {
                using var client = CreateDynamoClient(parameters);
                return await ScanAllAsync(client, parameters, tableName);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DynamoDB: failed to read table '{Table}'.", tableName);
                throw new ConnectorException($"Failed to read DynamoDB table '{tableName}': {ex.Message}", ex, "dynamodb");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var tableName  = GetRequiredParam(parameters, "tableName");

            _logger.LogInformation("DynamoDB: discovering schema for table '{Table}'.", tableName);

            try
            {
                using var client = CreateDynamoClient(parameters);

                // Use DescribeTable to get key schema and attribute definitions
                var describeResponse = await client.DescribeTableAsync(new DescribeTableRequest
                {
                    TableName = tableName
                });

                var names = new List<string>();

                // Add key schema attributes
                foreach (var key in describeResponse.Table.KeySchema)
                {
                    if (!names.Contains(key.AttributeName))
                        names.Add(key.AttributeName);
                }

                // Add attribute definitions
                foreach (var attr in describeResponse.Table.AttributeDefinitions)
                {
                    if (!names.Contains(attr.AttributeName))
                        names.Add(attr.AttributeName);
                }

                // Also scan a single item to discover all fields
                var scanResponse = await client.ScanAsync(new ScanRequest
                {
                    TableName = tableName,
                    Limit = 1
                });

                if (scanResponse.Items.Count > 0)
                {
                    foreach (var key in scanResponse.Items[0].Keys)
                    {
                        if (!names.Contains(key))
                            names.Add(key);
                    }
                }

                return names;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DynamoDB: failed to discover schema for table '{Table}'.", tableName);
                throw new ConnectorException($"Failed to discover DynamoDB schema for '{tableName}': {ex.Message}", ex, "dynamodb");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var tableName  = GetRequiredParam(parameters, "tableName");

            _logger.LogInformation("DynamoDB: dry run preview for table '{Table}' (sample={SampleSize}).", tableName, sampleSize);

            try
            {
                using var client = CreateDynamoClient(parameters);

                var scanRequest = new ScanRequest
                {
                    TableName = tableName,
                    Limit = sampleSize
                };

                var filterExpression = GetStringParam(parameters, "filterExpression");
                if (!string.IsNullOrWhiteSpace(filterExpression))
                {
                    scanRequest.FilterExpression = filterExpression;
                }

                var response = await client.ScanAsync(scanRequest);
                return response.Items.Select(ConvertItemToExpando).ToList<object>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DynamoDB: dry run preview failed for table '{Table}'.", tableName);
                throw new ConnectorException($"DynamoDB dry run preview failed for '{tableName}': {ex.Message}", ex, "dynamodb");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);

            try
            {
                using var client = CreateDynamoClient(parameters);

                var tables = new List<string>();
                string? lastEvaluated = null;

                do
                {
                    var listRequest = new ListTablesRequest();
                    if (lastEvaluated != null)
                        listRequest.ExclusiveStartTableName = lastEvaluated;

                    var response = await client.ListTablesAsync(listRequest);
                    tables.AddRange(response.TableNames);
                    lastEvaluated = response.LastEvaluatedTableName;
                }
                while (lastEvaluated != null);

                return tables;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "DynamoDB: failed to list tables; returning empty list.");
                return Array.Empty<string>();
            }
        }

        // ── Scan with pagination ─────────────────────────────────────────────

        private async Task<List<object>> ScanAllAsync(
            AmazonDynamoDBClient client, Dictionary<string, object> parameters, string tableName)
        {
            var results = new List<object>();
            Dictionary<string, AttributeValue>? exclusiveStartKey = null;

            var filterExpression = GetStringParam(parameters, "filterExpression");

            do
            {
                var scanRequest = new ScanRequest
                {
                    TableName = tableName,
                    Limit = 1000
                };

                if (!string.IsNullOrWhiteSpace(filterExpression))
                {
                    scanRequest.FilterExpression = filterExpression;
                }

                if (exclusiveStartKey != null)
                {
                    scanRequest.ExclusiveStartKey = exclusiveStartKey;
                }

                var response = await client.ScanAsync(scanRequest);

                foreach (var item in response.Items)
                {
                    results.Add(ConvertItemToExpando(item));
                }

                exclusiveStartKey = response.LastEvaluatedKey?.Count > 0 ? response.LastEvaluatedKey : null;
            }
            while (exclusiveStartKey != null);

            _logger.LogInformation("DynamoDB: read {Count} items from table '{Table}'.",
                results.Count, tableName);

            return results;
        }

        // ── DynamoDB client creation ─────────────────────────────────────────

        private static AmazonDynamoDBClient CreateDynamoClient(Dictionary<string, object> parameters)
        {
            var accessKeyId     = GetStringParam(parameters, "accessKeyId");
            var secretAccessKey = GetStringParam(parameters, "secretAccessKey");
            var region          = GetStringParam(parameters, "region") ?? "us-east-1";

            var regionEndpoint = RegionEndpoint.GetBySystemName(region);

            if (!string.IsNullOrWhiteSpace(accessKeyId) && !string.IsNullOrWhiteSpace(secretAccessKey))
            {
                var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
                return new AmazonDynamoDBClient(credentials, regionEndpoint);
            }

            // Fall back to default credentials (environment variables, instance profile, etc.)
            return new AmazonDynamoDBClient(regionEndpoint);
        }

        // ── DynamoDB AttributeValue → ExpandoObject conversion ───────────────

        private static object ConvertItemToExpando(Dictionary<string, AttributeValue> item)
        {
            IDictionary<string, object> row = new ExpandoObject();

            foreach (var kvp in item)
            {
                row[kvp.Key] = ConvertAttributeValue(kvp.Value);
            }

            return row;
        }

        private static object ConvertAttributeValue(AttributeValue attr)
        {
            if (attr.S != null)
                return attr.S;
            if (attr.N != null)
            {
                if (long.TryParse(attr.N, out var l))
                    return l;
                if (double.TryParse(attr.N, out var d))
                    return d;
                return attr.N;
            }
            if (attr.BOOL.HasValue && attr.BOOL.Value)
                return attr.BOOL.Value;
            if (attr.NULL.HasValue && attr.NULL.Value)
                return string.Empty;
            if (attr.SS?.Count > 0)
                return string.Join(", ", attr.SS);
            if (attr.NS?.Count > 0)
                return string.Join(", ", attr.NS);
            if (attr.L?.Count > 0)
                return string.Join(", ", attr.L.Select(ConvertAttributeValue));
            if (attr.M?.Count > 0)
            {
                IDictionary<string, object> nested = new ExpandoObject();
                foreach (var kvp in attr.M)
                {
                    nested[kvp.Key] = ConvertAttributeValue(kvp.Value);
                }
                return JsonSerializer.Serialize(nested);
            }
            if (attr.B != null)
                return Convert.ToBase64String(attr.B.ToArray());

            return string.Empty;
        }

        // ── Connection-string merge ──────────────────────────────────────────

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Connection string format: {"accessKeyId":"...","secretAccessKey":"...","region":"us-east-1"}
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

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> parameters, string key)
        {
            var value = GetStringParam(parameters, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"DynamoDB connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "dynamodb");
            return value;
        }
    }
}
