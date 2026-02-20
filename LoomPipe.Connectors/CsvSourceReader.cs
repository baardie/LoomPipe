// ReSharper disable NullableWarningSuppressionIsUsed
#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// CSV file source connector implementing ISourceReader.
    /// </summary>
    public class CsvSourceReader : ISourceReader
    {
        private readonly ILogger<CsvSourceReader> _logger;

        public CsvSourceReader(ILogger<CsvSourceReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading CSV file from '{Path}'.", config.ConnectionString);
            try
            {
                var path = config.ConnectionString;
                var lines = await File.ReadAllLinesAsync(path);
                return ParseCsv(lines);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read CSV file from '{Path}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to read CSV file: {ex.Message}", ex, "csv");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for CSV file at '{Path}'.", config.ConnectionString);
            try
            {
                var path = config.ConnectionString;
                var firstLine = (await File.ReadLinesAsync(path).ToListAsync()).FirstOrDefault();
                return firstLine?.Split(',') ?? Array.Empty<string>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for CSV file at '{Path}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to discover schema for CSV file: {ex.Message}", ex, "csv");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Starting dry run for CSV file at '{Path}'.", config.ConnectionString);
            try
            {
                var path = config.ConnectionString;
                var lines = (await File.ReadLinesAsync(path).Take(sampleSize + 1).ToListAsync());
                return ParseCsv(lines);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry run failed for CSV file at '{Path}'.", config.ConnectionString);
                throw new ConnectorException($"Dry run failed for CSV file: {ex.Message}", ex, "csv");
            }
        }

        private IEnumerable<object> ParseCsv(IList<string> lines)
        {
            if (lines == null || lines.Count < 2) yield break;
            var headers = lines[0].Split(',');
            foreach (var line in lines.Skip(1))
            {
                var values = line.Split(',');
                var expando = new ExpandoObject();
                var dict = (IDictionary<string, object>)expando;
                for (int i = 0; i < headers.Length && i < values.Length; i++)
                    dict[headers[i]] = values[i];
                yield return expando;
            }
        }
    }
}
