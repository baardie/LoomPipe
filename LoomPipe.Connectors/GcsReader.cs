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
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Google Cloud Storage source connector implementing ISourceReader.
    /// Reads CSV, JSON, and JSONL files from GCS buckets.
    /// </summary>
    public class GcsReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<GcsReader> _logger;

        public GcsReader(HttpClient httpClient, ILogger<GcsReader> logger)
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
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Reading GCS source from bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);

            try
            {
                var client = CreateClient(parameters);
                var rows = await ReadAllRows(client, parameters, bucket, prefix);

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
                _logger.LogError(ex, "Failed to read from GCS bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);
                throw new ConnectorException($"Failed to read from GCS: {ex.Message}", ex, "gcs");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Discovering schema for GCS source bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);

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
                _logger.LogError(ex, "Failed to discover schema for GCS bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);
                throw new ConnectorException($"Failed to discover GCS schema: {ex.Message}", ex, "gcs");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Dry run for GCS source bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for GCS bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);
                throw new ConnectorException($"Dry run failed for GCS: {ex.Message}", ex, "gcs");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var prefix = GetStringParam(parameters, "prefix") ?? "";
            _logger.LogInformation("Listing GCS resources in bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);

            try
            {
                var client = CreateClient(parameters);
                var objectNames = new List<string>();

                var objects = client.ListObjectsAsync(bucket, prefix);
                await foreach (var obj in objects)
                {
                    objectNames.Add(obj.Name);
                }

                return objectNames;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list GCS resources in bucket '{Bucket}'.", bucket);
                throw new ConnectorException($"Failed to list GCS resources: {ex.Message}", ex, "gcs");
            }
        }

        // -- Private helpers -----------------------------------------------------------

        /// <summary>
        /// Reads all rows from objects in a GCS bucket matching the prefix and optional file pattern.
        /// </summary>
        private async Task<List<object>> ReadAllRows(
            StorageClient client,
            Dictionary<string, object> parameters,
            string bucket,
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
            var objects = client.ListObjectsAsync(bucket, prefix);

            await foreach (var obj in objects)
            {
                if (obj.Size == 0) continue;

                // Apply file pattern filter if specified
                var fileName = Path.GetFileName(obj.Name);
                if (patternRegex != null && !patternRegex.IsMatch(fileName))
                    continue;

                var content = await DownloadObjectAsString(client, bucket, obj.Name);
                allRows.AddRange(ParseContent(content, parameters, obj.Name));
            }

            return allRows;
        }

        private static async Task<string> DownloadObjectAsString(StorageClient client, string bucket, string objectName)
        {
            using var memoryStream = new MemoryStream();
            await client.DownloadObjectAsync(bucket, objectName, memoryStream);
            memoryStream.Position = 0;
            using var reader = new StreamReader(memoryStream, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        /// <summary>
        /// Parses file content based on the configured file format or file extension.
        /// </summary>
        private static IEnumerable<object> ParseContent(string content, Dictionary<string, object> parameters, string objectName)
        {
            var format = GetStringParam(parameters, "fileFormat");
            if (string.IsNullOrEmpty(format))
            {
                // Detect from extension
                var ext = Path.GetExtension(objectName).ToLowerInvariant();
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
                    "GCS JSON file must contain a JSON array at the root.",
                    new InvalidOperationException("Expected a JSON array at the root."),
                    "gcs");

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
