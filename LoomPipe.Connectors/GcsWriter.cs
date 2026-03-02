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
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Google Cloud Storage destination connector implementing IDestinationWriter.
    /// Writes CSV, JSON, and JSONL files to GCS buckets.
    /// </summary>
    public class GcsWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<GcsWriter> _logger;

        public GcsWriter(HttpClient httpClient, ILogger<GcsWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var key = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Writing to GCS bucket '{Bucket}', key '{Key}'.", bucket, key);

            try
            {
                var recordList = records.ToList();
                if (recordList.Count == 0)
                {
                    _logger.LogWarning("No records to write to GCS.");
                    return;
                }

                var format = (GetStringParam(parameters, "fileFormat") ?? "csv").ToLowerInvariant();
                var content = format switch
                {
                    "json" => SerializeAsJson(recordList),
                    "jsonl" => SerializeAsJsonLines(recordList),
                    _ => SerializeAsCsv(recordList, parameters)
                };

                var contentType = format switch
                {
                    "json" => "application/json",
                    "jsonl" => "application/x-ndjson",
                    _ => "text/csv"
                };

                var client = CreateClient(parameters);
                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                await client.UploadObjectAsync(bucket, key, contentType, stream);

                _logger.LogInformation("Successfully wrote {Count} records to GCS bucket '{Bucket}', key '{Key}'.",
                    recordList.Count, bucket, key);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to GCS bucket '{Bucket}', key '{Key}'.", bucket, key);
                throw new ConnectorException($"Failed to write to GCS: {ex.Message}", ex, "gcs");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // GCS is a file store — no strict schema to validate against.
            // We accept any set of fields.
            _logger.LogInformation("Schema validation for GCS always returns true (file-based storage).");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config,
            IEnumerable<object> records,
            int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for GCS writer.");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // -- Serialization helpers -----------------------------------------------------

        private static string SerializeAsCsv(List<object> records, Dictionary<string, object> parameters)
        {
            var delimiter = GetStringParam(parameters, "delimiter") ?? ",";
            var delimChar = delimiter.Length > 0 ? delimiter[0] : ',';

            var sb = new StringBuilder();

            var firstDict = records[0] as IDictionary<string, object>;
            if (firstDict == null) return string.Empty;

            var headers = firstDict.Keys.ToList();
            sb.AppendLine(string.Join(delimChar, headers));

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

        // -- GCS client creation -------------------------------------------------------

        private static StorageClient CreateClient(Dictionary<string, object> parameters)
        {
            var serviceAccountJson = GetStringParam(parameters, "serviceAccountJson");

            if (!string.IsNullOrEmpty(serviceAccountJson))
            {
                var credential = GoogleCredential.FromJson(serviceAccountJson);
                return StorageClient.Create(credential);
            }

            // Fall back to application default credentials
            return StorageClient.Create();
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
