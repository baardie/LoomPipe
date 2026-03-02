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
using Renci.SshNet;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// SFTP source connector implementing ISourceReader.
    /// Reads CSV, JSON, and JSONL files from SFTP servers.
    /// </summary>
    public class SftpReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SftpReader> _logger;

        public SftpReader(HttpClient httpClient, ILogger<SftpReader> logger)
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
            var host = GetStringParam(parameters, "host") ?? "";
            var remotePath = GetStringParam(parameters, "remotePath") ?? "/";
            _logger.LogInformation("Reading SFTP source from host '{Host}', path '{RemotePath}'.", host, remotePath);

            try
            {
                using var client = CreateClient(parameters);
                client.Connect();

                var rows = ReadAllRows(client, parameters, remotePath);

                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    rows = rows.Where(r =>
                    {
                        var dict = (IDictionary<string, object>)r;
                        if (!dict.TryGetValue(watermarkField, out var val)) return false;
                        return string.Compare(val?.ToString(), watermarkValue, StringComparison.Ordinal) > 0;
                    }).ToList();
                }

                return await Task.FromResult<IEnumerable<object>>(rows);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);
                throw new ConnectorException($"Failed to read from SFTP: {ex.Message}", ex, "sftp");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var host = GetStringParam(parameters, "host") ?? "";
            var remotePath = GetStringParam(parameters, "remotePath") ?? "/";
            _logger.LogInformation("Discovering schema for SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);

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
                _logger.LogError(ex, "Failed to discover schema for SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);
                throw new ConnectorException($"Failed to discover SFTP schema: {ex.Message}", ex, "sftp");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var parameters = MergeConnectionString(config);
            var host = GetStringParam(parameters, "host") ?? "";
            var remotePath = GetStringParam(parameters, "remotePath") ?? "/";
            _logger.LogInformation("Dry run for SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);

            try
            {
                var records = await ReadAsync(config);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for SFTP host '{Host}', path '{RemotePath}'.", host, remotePath);
                throw new ConnectorException($"Dry run failed for SFTP: {ex.Message}", ex, "sftp");
            }
        }

        public async Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            var parameters = MergeConnectionString(config);
            var host = GetStringParam(parameters, "host") ?? "";
            var remotePath = GetStringParam(parameters, "remotePath") ?? "/";
            _logger.LogInformation("Listing SFTP resources on host '{Host}', path '{RemotePath}'.", host, remotePath);

            try
            {
                using var client = CreateClient(parameters);
                client.Connect();

                var fileNames = new List<string>();
                var files = client.ListDirectory(remotePath);
                foreach (var file in files)
                {
                    if (file.IsDirectory || file.Name == "." || file.Name == "..") continue;
                    fileNames.Add(file.FullName);
                }

                return await Task.FromResult<IEnumerable<string>>(fileNames);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list SFTP resources on host '{Host}'.", host);
                throw new ConnectorException($"Failed to list SFTP resources: {ex.Message}", ex, "sftp");
            }
        }

        // -- Private helpers -----------------------------------------------------------

        /// <summary>
        /// Reads all rows from files on the SFTP server matching the remote path and optional file pattern.
        /// </summary>
        private List<object> ReadAllRows(
            SftpClient client,
            Dictionary<string, object> parameters,
            string remotePath)
        {
            var filePattern = GetStringParam(parameters, "filePattern");
            Regex? patternRegex = null;
            if (!string.IsNullOrEmpty(filePattern))
            {
                var regexPattern = "^" + Regex.Escape(filePattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
                patternRegex = new Regex(regexPattern, RegexOptions.IgnoreCase);
            }

            var allRows = new List<object>();

            // Check if remotePath is a file or directory
            var attrs = client.GetAttributes(remotePath);
            if (!attrs.IsDirectory)
            {
                // Single file
                var content = DownloadFileAsString(client, remotePath);
                allRows.AddRange(ParseContent(content, parameters, remotePath));
                return allRows;
            }

            // Directory — list and download each file
            var files = client.ListDirectory(remotePath);
            foreach (var file in files)
            {
                if (file.IsDirectory || file.Name == "." || file.Name == "..") continue;
                if (file.Length == 0) continue;

                // Apply file pattern filter if specified
                if (patternRegex != null && !patternRegex.IsMatch(file.Name))
                    continue;

                var content = DownloadFileAsString(client, file.FullName);
                allRows.AddRange(ParseContent(content, parameters, file.Name));
            }

            return allRows;
        }

        private static string DownloadFileAsString(SftpClient client, string remotePath)
        {
            using var memoryStream = new MemoryStream();
            client.DownloadFile(remotePath, memoryStream);
            memoryStream.Position = 0;
            using var reader = new StreamReader(memoryStream, Encoding.UTF8);
            return reader.ReadToEnd();
        }

        /// <summary>
        /// Parses file content based on the configured file format or file extension.
        /// </summary>
        private static IEnumerable<object> ParseContent(string content, Dictionary<string, object> parameters, string fileName)
        {
            var format = GetStringParam(parameters, "fileFormat");
            if (string.IsNullOrEmpty(format))
            {
                // Detect from extension
                var ext = Path.GetExtension(fileName).ToLowerInvariant();
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
                    "SFTP JSON file must contain a JSON array at the root.",
                    new InvalidOperationException("Expected a JSON array at the root."),
                    "sftp");

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
