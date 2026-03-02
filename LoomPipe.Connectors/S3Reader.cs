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
    /// Amazon S3 source connector implementing ISourceReader.
    /// Reads CSV, JSON, and JSONL files from S3 buckets. Supports S3-compatible services (MinIO, etc.)
    /// via the optional endpointUrl parameter.
    /// </summary>
    public class S3Reader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<S3Reader> _logger;

        public S3Reader(HttpClient httpClient, ILogger<S3Reader> logger)
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
            var key = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Reading S3 source from bucket '{Bucket}', key/prefix '{Key}'.", bucket, key);

            try
            {
                using var client = CreateClient(parameters);
                var rows = await ReadAllRows(client, parameters, bucket, key);

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
                _logger.LogError(ex, "Failed to read from S3 bucket '{Bucket}', key '{Key}'.", bucket, key);
                throw new ConnectorException($"Failed to read from S3: {ex.Message}", ex, "s3");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var key = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Discovering schema for S3 source bucket '{Bucket}', key '{Key}'.", bucket, key);

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
                _logger.LogError(ex, "Failed to discover schema for S3 bucket '{Bucket}', key '{Key}'.", bucket, key);
                throw new ConnectorException($"Failed to discover S3 schema: {ex.Message}", ex, "s3");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var key = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Dry run for S3 source bucket '{Bucket}', key '{Key}'.", bucket, key);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for S3 bucket '{Bucket}', key '{Key}'.", bucket, key);
                throw new ConnectorException($"Dry run failed for S3: {ex.Message}", ex, "s3");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var bucket = GetStringParam(parameters, "bucket") ?? "";
            var prefix = GetStringParam(parameters, "key") ?? "";
            _logger.LogInformation("Listing S3 resources in bucket '{Bucket}', prefix '{Prefix}'.", bucket, prefix);

            try
            {
                using var client = CreateClient(parameters);
                var request = new ListObjectsV2Request
                {
                    BucketName = bucket,
                    Prefix = prefix
                };

                var keys = new List<string>();
                ListObjectsV2Response response;
                do
                {
                    response = await client.ListObjectsV2Async(request);
                    keys.AddRange(response.S3Objects.Select(o => o.Key));
                    request.ContinuationToken = response.NextContinuationToken;
                } while (response.IsTruncated == true);

                return keys;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list S3 resources in bucket '{Bucket}'.", bucket);
                throw new ConnectorException($"Failed to list S3 resources: {ex.Message}", ex, "s3");
            }
        }

        // -- Private helpers -----------------------------------------------------------

        /// <summary>
        /// Reads all rows from either a single S3 object (if key points to a file) or
        /// from all objects under a prefix.
        /// </summary>
        private async Task<List<object>> ReadAllRows(
            AmazonS3Client client,
            Dictionary<string, object> parameters,
            string bucket,
            string key)
        {
            // First try as a single object
            if (!string.IsNullOrEmpty(key) && !key.EndsWith("/"))
            {
                try
                {
                    var content = await DownloadObjectAsString(client, bucket, key);
                    return ParseContent(content, parameters).ToList();
                }
                catch (AmazonS3Exception s3Ex) when (s3Ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    // Key might be a prefix, fall through to listing
                }
            }

            // Treat as prefix — list and download each file
            var listRequest = new ListObjectsV2Request
            {
                BucketName = bucket,
                Prefix = key
            };

            var allRows = new List<object>();
            ListObjectsV2Response listResponse;
            do
            {
                listResponse = await client.ListObjectsV2Async(listRequest);
                foreach (var obj in listResponse.S3Objects.Where(o => o.Size > 0))
                {
                    var content = await DownloadObjectAsString(client, bucket, obj.Key);
                    allRows.AddRange(ParseContent(content, parameters));
                }
                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated == true);

            return allRows;
        }

        private static async Task<string> DownloadObjectAsString(AmazonS3Client client, string bucket, string key)
        {
            var request = new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            };
            using var response = await client.GetObjectAsync(request);
            using var reader = new StreamReader(response.ResponseStream, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        /// <summary>
        /// Parses file content based on the configured file format (csv, json, jsonl).
        /// </summary>
        private static IEnumerable<object> ParseContent(string content, Dictionary<string, object> parameters)
        {
            var format = (GetStringParam(parameters, "fileFormat") ?? "csv").ToLowerInvariant();
            return format switch
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
                // Generate column names Col_0, Col_1, ...
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
                    "S3 JSON file must contain a JSON array at the root.",
                    new InvalidOperationException("Expected a JSON array at the root."),
                    "s3");

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
