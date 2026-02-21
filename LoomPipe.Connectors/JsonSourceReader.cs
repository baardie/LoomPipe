#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// JSON source connector. Accepts either:
    ///   • Inline JSON text stored in <see cref="DataSourceConfig.ConnectionString"/>
    ///     (when <c>Parameters["jsonMode"] == "inline"</c>), or
    ///   • A server-side file path stored in <see cref="DataSourceConfig.ConnectionString"/>
    ///     (when <c>Parameters["jsonMode"] == "file"</c>).
    ///
    /// The root JSON value may be an array of objects <c>[{...},...]</c> or a single
    /// object <c>{...}</c> (which is treated as a one-record array).
    /// </summary>
    public class JsonSourceReader : ISourceReader
    {
        private readonly ILogger<JsonSourceReader> _logger;

        public JsonSourceReader(ILogger<JsonSourceReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config, string? watermarkField = null, string? watermarkValue = null)
        {
            var (mode, label) = ResolveMode(config);
            _logger.LogInformation("JSON source reading ({Mode}): {Label}", mode, label);
            try
            {
                var json = await GetJsonAsync(config, mode);
                return ParseJson(json);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read JSON source ({Mode}): {Label}", mode, label);
                throw new ConnectorException($"Failed to read JSON source: {ex.Message}", ex, "json");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogDebug("Discovering schema for JSON source.");
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
                throw new ConnectorException($"Failed to discover JSON schema: {ex.Message}", ex, "json");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogDebug("Dry run preview for JSON source (sample={SampleSize}).", sampleSize);
            var records = await ReadAsync(config);
            return records.Take(sampleSize);
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static (string mode, string label) ResolveMode(DataSourceConfig config)
        {
            var mode = "inline";
            if (config.Parameters?.TryGetValue("jsonMode", out var raw) == true)
                mode = raw?.ToString() ?? "inline";

            var label = mode == "file"
                ? config.ConnectionString
                : $"inline ({config.ConnectionString.Length} chars)";

            return (mode, label);
        }

        private static async Task<string> GetJsonAsync(DataSourceConfig config, string mode)
        {
            if (mode == "file")
            {
                if (!File.Exists(config.ConnectionString))
                    throw new ConnectorException(
                        $"JSON file not found: '{config.ConnectionString}'",
                        new FileNotFoundException(config.ConnectionString),
                        "json");

                return await File.ReadAllTextAsync(config.ConnectionString);
            }

            // Inline — the connection string IS the JSON text
            return config.ConnectionString;
        }

        private static IEnumerable<object> ParseJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                throw new ConnectorException("JSON source is empty.", new InvalidOperationException("Empty JSON"), "json");

            JsonDocument doc;
            try { doc = JsonDocument.Parse(json); }
            catch (JsonException jex)
            {
                throw new ConnectorException($"Invalid JSON: {jex.Message}", jex, "json");
            }

            using (doc)
            {
                return doc.RootElement.ValueKind switch
                {
                    JsonValueKind.Array  => ParseArray(doc.RootElement),
                    JsonValueKind.Object => new[] { ParseObject(doc.RootElement) },
                    _                    => throw new ConnectorException(
                        "JSON root must be an array [...] or an object {...}.",
                        new InvalidOperationException($"Unexpected root kind: {doc.RootElement.ValueKind}"),
                        "json")
                };
            }
        }

        private static List<object> ParseArray(JsonElement array)
        {
            var results = new List<object>();
            foreach (var element in array.EnumerateArray())
            {
                results.Add(element.ValueKind == JsonValueKind.Object
                    ? ParseObject(element)
                    : WrapPrimitive(element));
            }
            return results;
        }

        private static IDictionary<string, object> ParseObject(JsonElement element)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            foreach (var prop in element.EnumerateObject())
            {
                expando[prop.Name] = MapValue(prop.Value);
            }
            return expando;
        }

        private static object MapValue(JsonElement v) => v.ValueKind switch
        {
            JsonValueKind.String  => (object)(v.GetString() ?? string.Empty),
            JsonValueKind.Number  => v.TryGetInt64(out var l) ? l : v.GetDouble(),
            JsonValueKind.True    => true,
            JsonValueKind.False   => false,
            JsonValueKind.Null    => string.Empty,
            // Nested objects/arrays are serialised to a JSON string for flat pipeline processing
            _                     => v.GetRawText(),
        };

        private static object WrapPrimitive(JsonElement v)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            expando["value"] = MapValue(v);
            return expando;
        }
    }
}
