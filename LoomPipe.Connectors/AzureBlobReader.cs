#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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
    /// Azure Blob Storage source connector implementing ISourceReader.
    /// Reads CSV, JSON, and JSONL files from Azure Blob containers.
    /// </summary>
    public class AzureBlobReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<AzureBlobReader> _logger;

        public AzureBlobReader(HttpClient httpClient, ILogger<AzureBlobReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var parameters = MergeConnectionString(config);
            var container = GetStringParam(parameters, "container") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Reading Azure Blob source from container '{Container}', prefix '{Prefix}'.", container, prefix);

            try
            {
                var containerClient = CreateContainerClient(parameters);
                var rows = await ReadAllRows(containerClient, parameters, prefix);

                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    rows = rows.Where(r =>
                    {
                        var dict = (IDictionary<string, object>)r;
                        if (!dict.TryGetValue(watermarkField, out var val)) return false;
                        return string.Compare(val?.ToString(), watermarkValue, StringComparison.Ordinal) > 0;
                    }).ToList();
                }

                return rows;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from Azure Blob container '{Container}', prefix '{Prefix}'.", container, prefix);
                throw new ConnectorException($"Failed to read from Azure Blob: {ex.Message}", ex, "azureblob");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var container = GetStringParam(parameters, "container") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Discovering schema for Azure Blob container '{Container}', prefix '{Prefix}'.", container, prefix);

            try
            {
                var records = await ReadAsync(config);
                var first = records.FirstOrDefault();
                if (first is IDictionary<string, object> dict)
                    return dict.Keys;
                return Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for Azure Blob container '{Container}', prefix '{Prefix}'.", container, prefix);
                throw new ConnectorException($"Failed to discover Azure Blob schema: {ex.Message}", ex, "azureblob");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var container = GetStringParam(parameters, "container") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Dry run for Azure Blob container '{Container}', prefix '{Prefix}'.", container, prefix);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for Azure Blob container '{Container}', prefix '{Prefix}'.", container, prefix);
                throw new ConnectorException($"Dry run failed for Azure Blob: {ex.Message}", ex, "azureblob");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var container = GetStringParam(parameters, "container") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Listing Azure Blob resources in container '{Container}', prefix '{Prefix}'.", container, prefix);

            try
            {
                var containerClient = CreateContainerClient(parameters);
                var blobNames = new List<string>();

                await foreach (var blobItem in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix, default))
                {
                    blobNames.Add(blobItem.Name);
                }

                return blobNames;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list Azure Blob resources in container '{Container}'.", container);
                throw new ConnectorException($"Failed to list Azure Blob resources: {ex.Message}", ex, "azureblob");
            }
        }

        // -- Private helpers -----------------------------------------------------------

        /// <summary>
        /// Reads all rows from blobs in an Azure container matching the prefix and optional file pattern.
        /// </summary>
        private async Task<List<object>> ReadAllRows(
            BlobContainerClient containerClient,
            Dictionary<string, object> parameters,
            string prefix)
        {
            var filePattern = GetStringParam(parameters, "filePattern");
            Regex? patternRegex = null;
            if (!string.IsNullOrEmpty(filePattern))
            {
                var regexPattern = "^" + Regex.Escape(filePattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
                patternRegex = new Regex(regexPattern, RegexOptions.IgnoreCase);
            }

            var allRows = new List<object>();

            await foreach (var blobItem in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix, default))
            {
                if (blobItem.Properties.ContentLength == 0) continue;

                // Apply file pattern filter if specified
                var fileName = Path.GetFileName(blobItem.Name);
                if (patternRegex != null && !patternRegex.IsMatch(fileName))
                    continue;

                var blobClient = containerClient.GetBlobClient(blobItem.Name);
                var content = await DownloadBlobAsString(blobClient);
                allRows.AddRange(ParseContent(content, parameters, blobItem.Name));
            }

            return allRows;
        }

        private static async Task<string> DownloadBlobAsString(BlobClient blobClient)
        {
            var response = await blobClient.DownloadContentAsync();
            return response.Value.Content.ToString();
        }

        /// <summary>
        /// Parses file content based on the configured file format or file extension.
        /// </summary>
        private static IEnumerable<object> ParseContent(string content, Dictionary<string, object> parameters, string blobName)
        {
            var format = GetStringParam(parameters, "fileFormat");
            if (string.IsNullOrEmpty(format))
            {
                // Detect from extension
                var ext = Path.GetExtension(blobName).ToLowerInvariant();
                format = ext switch
                {
                    ".json" => "json",
                    ".jsonl" => "jsonl",
                    _ => "csv"
                };
            }

            return format.ToLowerInvariant() switch
            {
                "json" => ParseJson(content),
                "jsonl" => ParseJsonLines(content),
                _ => ParseCsv(content, parameters)
            };
        }

        // -- CSV parsing ---------------------------------------------------------------

        private static IEnumerable<object> ParseCsv(string content, Dictionary<string, object> parameters)
        {
            var delimiter = GetStringParam(parameters, "delimiter") ?? ",";
            var hasHeader = (GetStringParam(parameters, "headerRow") ?? "true").Equals("true", StringComparison.OrdinalIgnoreCase);
            var delimChar = delimiter.Length > 0 ? delimiter[0] : ',';

            var lines = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
            if (lines.Length == 0) return Enumerable.Empty<object>();

            string[] headers;
            int startLine;

            if (hasHeader && lines.Length > 0)
            {
                headers = lines[0].Split(delimChar);
                startLine = 1;
            }
            else
            {
                var firstLineFields = lines[0].Split(delimChar);
                headers = Enumerable.Range(0, firstLineFields.Length).Select(i => $"Col_{i}").ToArray();
                startLine = 0;
            }

            var rows = new List<object>();
            for (int i = startLine; i < lines.Length; i++)
            {
                var values = lines[i].Split(delimChar);
                IDictionary<string, object> expando = new ExpandoObject();
                for (int j = 0; j < headers.Length && j < values.Length; j++)
                    expando[headers[j]] = values[j];
                rows.Add(expando);
            }
            return rows;
        }

        // -- JSON parsing --------------------------------------------------------------

        private static IEnumerable<object> ParseJson(string content)
        {
            using var doc = JsonDocument.Parse(content);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                throw new ConnectorException(
                    "Azure Blob JSON file must contain a JSON array at the root.",
                    new InvalidOperationException("Expected a JSON array at the root."),
                    "azureblob");

            var rows = new List<object>();
            foreach (var element in doc.RootElement.EnumerateArray())
            {
                rows.Add(JsonElementToExpando(element));
            }
            return rows;
        }

        private static IEnumerable<object> ParseJsonLines(string content)
        {
            var lines = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
            var rows = new List<object>();
            foreach (var line in lines)
            {
                using var doc = JsonDocument.Parse(line);
                rows.Add(JsonElementToExpando(doc.RootElement));
            }
            return rows;
        }

        private static ExpandoObject JsonElementToExpando(JsonElement element)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            foreach (var prop in element.EnumerateObject())
            {
                expando[prop.Name] = prop.Value.ValueKind switch
                {
                    JsonValueKind.String => (object)(prop.Value.GetString() ?? string.Empty),
                    JsonValueKind.Number => prop.Value.TryGetInt32(out var i) ? i : prop.Value.GetDouble(),
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    _ => prop.Value.ToString()
                };
            }
            return (ExpandoObject)expando;
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
