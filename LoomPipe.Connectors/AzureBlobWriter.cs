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
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Azure Blob Storage destination connector implementing IDestinationWriter.
    /// Writes CSV, JSON, and JSONL files to Azure Blob containers.
    /// </summary>
    public class AzureBlobWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<AzureBlobWriter> _logger;

        public AzureBlobWriter(HttpClient httpClient, ILogger<AzureBlobWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var parameters = MergeConnectionString(config);
            var container = GetStringParam(parameters, "container") ?? "";
            var blobName = GetStringParam(parameters, "blobName") ?? "";
            _logger.LogInformation("Writing to Azure Blob container '{Container}', blob '{BlobName}'.", container, blobName);

            try
            {
                var recordList = records.ToList();
                if (recordList.Count == 0)
                {
                    _logger.LogWarning("No records to write to Azure Blob.");
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

                var containerClient = CreateContainerClient(parameters);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                await blobClient.UploadAsync(stream, new BlobUploadOptions
                {
                    HttpHeaders = new BlobHttpHeaders { ContentType = contentType }
                });

                _logger.LogInformation("Successfully wrote {Count} records to Azure Blob container '{Container}', blob '{BlobName}'.",
                    recordList.Count, container, blobName);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to Azure Blob container '{Container}', blob '{BlobName}'.", container, blobName);
                throw new ConnectorException($"Failed to write to Azure Blob: {ex.Message}", ex, "azureblob");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // Azure Blob is a file store — no strict schema to validate against.
            // We accept any set of fields.
            _logger.LogInformation("Schema validation for Azure Blob always returns true (file-based storage).");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config,
            IEnumerable<object> records,
            int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for Azure Blob writer.");
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

        // -- Azure Blob client creation ------------------------------------------------

        private static BlobContainerClient CreateContainerClient(Dictionary<string, object> parameters)
        {
            var connectionString = GetStringParam(parameters, "connectionString") ?? "";
            var container = GetStringParam(parameters, "container") ?? "";

            var serviceClient = new BlobServiceClient(connectionString);
            return serviceClient.GetBlobContainerClient(container);
        }

        /// <summary>
        /// Merges connection-string JSON into the parameters dictionary.
        /// Parameters take precedence; connection string provides defaults.
        /// Supports both raw Azure connection strings and JSON-formatted connection strings.
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
            catch (JsonException)
            {
                // Not JSON — treat as a raw Azure Storage connection string
                if (!string.IsNullOrEmpty(config.ConnectionString))
                {
                    if (!merged.ContainsKey("connectionString") || string.IsNullOrWhiteSpace(GetStringParam(merged, "connectionString")))
                        merged["connectionString"] = config.ConnectionString;
                }
            }
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
