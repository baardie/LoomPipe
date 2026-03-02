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
using Renci.SshNet;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// SFTP destination connector implementing IDestinationWriter.
    /// Writes CSV, JSON, and JSONL files to SFTP servers.
    /// </summary>
    public class SftpWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SftpWriter> _logger;

        public SftpWriter(HttpClient httpClient, ILogger<SftpWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var parameters = MergeConnectionString(config);
            var host = GetStringParam(parameters, "host") ?? "";
            var remotePath = GetStringParam(parameters, "remotePath") ?? "";
            _logger.LogInformation("Writing to SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);

            try
            {
                var recordList = records.ToList();
                if (recordList.Count == 0)
                {
                    _logger.LogWarning("No records to write to SFTP.");
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
                client.Connect();

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                client.UploadFile(stream, remotePath, canOverride: true);

                _logger.LogInformation("Successfully wrote {Count} records to SFTP host '{Host}', path '{RemotePath}'.",
                    recordList.Count, host, remotePath);

                return;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);
                throw new ConnectorException($"Failed to write to SFTP: {ex.Message}", ex, "sftp");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // SFTP is a file store — no strict schema to validate against.
            // We accept any set of fields.
            _logger.LogInformation("Schema validation for SFTP always returns true (file-based storage).");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(
            DataSourceConfig config,
            IEnumerable<object> records,
            int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview for SFTP writer.");
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

        // -- SFTP client creation ------------------------------------------------------

        private static SftpClient CreateClient(Dictionary<string, object> parameters)
        {
            var host = GetStringParam(parameters, "host") ?? "";
            var portStr = GetStringParam(parameters, "port") ?? "22";
            var port = int.TryParse(portStr, out var p) ? p : 22;
            var username = GetStringParam(parameters, "username") ?? "";
            var password = GetStringParam(parameters, "password");
            var privateKey = GetStringParam(parameters, "privateKey");

            if (!string.IsNullOrEmpty(privateKey))
            {
                using var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(privateKey));
                var keyFile = string.IsNullOrEmpty(password)
                    ? new PrivateKeyFile(keyStream)
                    : new PrivateKeyFile(keyStream, password);
                return new SftpClient(host, port, username, keyFile);
            }

            return new SftpClient(host, port, username, password ?? "");
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
