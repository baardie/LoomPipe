// ReSharper disable NullableWarningSuppressionIsUsed
#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
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
    /// Supports Bearer token, API Key, and Basic auth; plus arbitrary custom request headers.
    /// Auth and header config is read from <see cref="DataSourceConfig.Parameters"/>.
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
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("Writing to webhook at '{Url}'.", config.ConnectionString);
            try
            {
                var url = config.ConnectionString;
                foreach (var record in records)
                {
                    using var request = new HttpRequestMessage(HttpMethod.Post, url);
                    ApplyAuth(request, config.Parameters);
                    ApplyCustomHeaders(request, config.Parameters);
                    var json = JsonSerializer.Serialize(record);
                    request.Content = new StringContent(json, Encoding.UTF8, "application/json");
                    var response = await _httpClient.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                }
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write to webhook at '{Url}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to write to webhook: {ex.Message}", ex, "webhook");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            _logger.LogWarning("Schema validation is not supported for webhooks. Returning true.");
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Starting dry run for webhook at '{Url}'.", config.ConnectionString);
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Auth + header helpers ─────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, Dictionary<string, object> parameters)
        {
            var authType = (GetStringParam(parameters, "authType") ?? "none").ToLowerInvariant();
            switch (authType)
            {
                case "bearer":
                    var token = GetStringParam(parameters, "authToken");
                    if (!string.IsNullOrEmpty(token))
                        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                    break;

                case "apikey":
                    var headerName = GetStringParam(parameters, "apiKeyHeader") ?? "X-Api-Key";
                    var apiKey     = GetStringParam(parameters, "apiKeyValue");
                    if (!string.IsNullOrEmpty(apiKey))
                        request.Headers.TryAddWithoutValidation(headerName, apiKey);
                    break;

                case "basic":
                    var user  = GetStringParam(parameters, "basicUsername") ?? string.Empty;
                    var pass  = GetStringParam(parameters, "basicPassword") ?? string.Empty;
                    var creds = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{pass}"));
                    request.Headers.Authorization = new AuthenticationHeaderValue("Basic", creds);
                    break;
            }
        }

        private static void ApplyCustomHeaders(HttpRequestMessage request, Dictionary<string, object> parameters)
        {
            var headers = GetHeadersParam(parameters);
            foreach (var (key, value) in headers)
                if (!string.IsNullOrWhiteSpace(key))
                    request.Headers.TryAddWithoutValidation(key, value);
        }

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : null;
            return v?.ToString();
        }

        private static Dictionary<string, string> GetHeadersParam(Dictionary<string, object> p)
        {
            if (!p.TryGetValue("headers", out var v)) return new Dictionary<string, string>();
            if (v is JsonElement je && je.ValueKind == JsonValueKind.Object)
                return je.Deserialize<Dictionary<string, string>>() ?? new Dictionary<string, string>();
            return new Dictionary<string, string>();
        }
    }
}
