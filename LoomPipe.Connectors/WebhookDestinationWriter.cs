// ReSharper disable NullableWarningSuppressionIsUsed
#nullable enable
using System;
using System.Collections.Generic;
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
    /// <summary>
    /// Webhook destination connector implementing IDestinationWriter.
    /// </summary>
    public class WebhookDestinationWriter : IDestinationWriter
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<WebhookDestinationWriter> _logger;

        public WebhookDestinationWriter(HttpClient httpClient, ILogger<WebhookDestinationWriter> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            _logger.LogInformation("Writing to webhook at '{Url}'.", config.ConnectionString);
            try
            {
                var url = config.ConnectionString;
                foreach (var record in records)
                {
                    var json = JsonSerializer.Serialize(record);
                    var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
                    var response = await _httpClient.PostAsync(url, content);
                    response.EnsureSuccessStatusCode();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to webhook at '{Url}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to write to webhook. See inner exception for details.", ex);
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // For webhooks, schema validation is typically not possible; always return true
            _logger.LogWarning("Schema validation is not supported for webhooks. Returning true.");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Starting dry run for webhook at '{Url}'.", config.ConnectionString);
            // For dry run, just return the first N records
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }
    }
}
