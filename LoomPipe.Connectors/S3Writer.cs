#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Amazon S3 destination connector implementing IDestinationWriter.
    /// Writes CSV, JSON, and JSONL files to S3 buckets. Supports S3-compatible services (MinIO, etc.)
    /// via the optional endpointUrl parameter.
    /// </summary>
    public class S3Writer : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<S3Writer> _logger;

        public S3Writer(HttpClient httpClient, ILogger<S3Writer> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var key = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Writing to S3 bucket '{Bucket}', key '{Key}'.", bucket, key);

            try
            {
                var recordList = records.ToList();
                if (recordList.Count == 0)
                {
                    _logger.LogWarning("No records to write to S3.");
                    return;
                }

                var format = (GetStringParam(parameters, "fileFormat") ?? "csv").ToLowerInvariant();
                var content = format switch
                {
                    "json" => SerializeAsJson(recordList),
                    "jsonl" => SerializeAsJsonLines(recordList),
                    _ => SerializeAsCsv(recordList, parameters)
                };

                using var client = CreateClient(parameters);
                var putRequest = new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = key,
                    ContentBody = content,
                    ContentType = format switch
                    {
                        "json" => "application/json",
                        "jsonl" => "application/x-ndjson",
                        _ => "text/csv"
                    }
                };

                await client.PutObjectAsync(putRequest);
                _logger.LogInformation("Successfully wrote {Count} records to S3 bucket '{Bucket}', key '{Key}'.",
                    recordList.Count, bucket, key);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to S3 bucket '{Bucket}', key '{Key}'.", bucket, key);
                throw new ConnectorException($"Failed to write to S3: {ex.Message}", ex, "s3");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // S3 is a file store — no strict schema to validate against.
            // We accept any set of fields.
            _logger.LogInformation("Schema validation for S3 always returns true (file-based storage).");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config,
            IEnumerable<object> records,
            int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for S3 writer.");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // -- Serialization helpers -----------------------------------------------------

        private static string SerializeAsCsv(List<object> records, Dictionary<string, object> parameters)
        {
            var delimiter = GetStringParam(parameters, "delimiter") ?? ",";
            var delimChar = delimiter.Length > 0 ? delimiter[0] : ',';

            var sb = new StringBuilder();

            // Extract headers from the first record
            var firstDict = records[0] as IDictionary<string, object>;
            if (firstDict == null) return string.Empty;

            var headers = firstDict.Keys.ToList();
            sb.AppendLine(string.Join(delimChar, headers));

            // Write data rows
            foreach (var record in records)
            {
                if (record is IDictionary<string, object> dict)
                {
                    var values = headers.Select(h => dict.TryGetValue(h, out var v) ? (v?.ToString() ?? "") : "");
                    sb.AppendLine(string.Join(delimChar, values));
                }
            }

            return sb.ToString();
        }

        private static string SerializeAsJson(List<object> records)
        {
            return JsonSerializer.Serialize(records, new JsonSerializerOptions { WriteIndented = true });
        }

        private static string SerializeAsJsonLines(List<object> records)
        {
            var sb = new StringBuilder();
            foreach (var record in records)
            {
                sb.AppendLine(JsonSerializer.Serialize(record));
            }
            return sb.ToString();
        }

        // -- S3 client creation --------------------------------------------------------

        private static AmazonS3Client CreateClient(Dictionary<string, object> parameters)
        {
            var accessKeyId = GetStringParam(parameters, "accessKeyId") ?? "";
            var secretAccessKey = GetStringParam(parameters, "secretAccessKey") ?? "";
            var region = GetStringParam(parameters, "region") ?? "us-east-1";
            var endpointUrl = GetStringParam(parameters, "endpointUrl");

            var config = new AmazonS3Config
            {
                RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region)
            };
            if (!string.IsNullOrEmpty(endpointUrl))
            {
                config.ServiceURL = endpointUrl;
                config.ForcePathStyle = true;
            }
            return new AmazonS3Client(accessKeyId, secretAccessKey, config);
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

        // -- Parameter helper ----------------------------------------------------------

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }
    }
}
