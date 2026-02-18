#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    public class RestSourceReader : ISourceReader
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<RestSourceReader> _logger;

        public RestSourceReader(HttpClient httpClient, ILogger<RestSourceReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading REST source from '{Url}'.", config.ConnectionString);
            try
            {
                var json = await _httpClient.GetStringAsync(config.ConnectionString);
                return ParseJson(json);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from REST source '{Url}'.", config.ConnectionString);
                throw new ConnectorException("Failed to read from REST source. See inner exception for details.", ex);
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for REST source at '{Url}'.", config.ConnectionString);
            try
            {
                var records = await ReadAsync(config);
                var first = records.FirstOrDefault();
                if (first is IDictionary<string, object> dict)
                    return dict.Keys;
                return Array.Empty<string>();
            }
            catch (ConnectorException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for REST source '{Url}'.", config.ConnectionString);
                throw new ConnectorException("Failed to discover REST schema. See inner exception for details.", ex);
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for REST source at '{Url}'.", config.ConnectionString);
            var records = await ReadAsync(config);
            return records.Take(sampleSize);
        }

        private static IEnumerable<object> ParseJson(string json)
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                throw new ConnectorException("REST source must return a JSON array.", new InvalidOperationException("Expected a JSON array at the root."));

            foreach (var element in doc.RootElement.EnumerateArray())
            {
                IDictionary<string, object> expando = new ExpandoObject();
                foreach (var prop in element.EnumerateObject())
                {
                    expando[prop.Name] = prop.Value.ValueKind switch
                    {
                        JsonValueKind.String  => (object)(prop.Value.GetString() ?? string.Empty),
                        JsonValueKind.Number  => prop.Value.TryGetInt32(out var i) ? i : prop.Value.GetDouble(),
                        JsonValueKind.True    => true,
                        JsonValueKind.False   => false,
                        _                     => prop.Value.ToString()
                    };
                }
                yield return expando;
            }
        }
    }
}
