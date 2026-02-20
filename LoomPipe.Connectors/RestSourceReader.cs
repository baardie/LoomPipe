#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
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
            _logger.LogInformation("Reading REST source from '{Url}' (auth={Auth}).",
                config.ConnectionString, GetStringParam(config.Parameters, "authType") ?? "none");
            try
            {
                using var request = BuildRequest(config.ConnectionString, config.Parameters);
                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();
                var json = await response.Content.ReadAsStringAsync();
                return ParseJson(json);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read from REST source '{Url}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to read from REST source: {ex.Message}", ex, "rest");
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
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to discover schema for REST source '{Url}'.", config.ConnectionString);
                throw new ConnectorException($"Failed to discover REST schema: {ex.Message}", ex, "rest");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for REST source at '{Url}'.", config.ConnectionString);
            var records = await ReadAsync(config);
            return records.Take(sampleSize);
        }

        // ── HTTP request builder ──────────────────────────────────────────────

        private static HttpRequestMessage BuildRequest(string url, Dictionary<string, object> parameters)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, parameters);
            ApplyCustomHeaders(request, parameters);
            return request;
        }

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

        // ── Parameter helpers — handle both JsonElement and plain strings ──────

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

        // ── JSON parser ───────────────────────────────────────────────────────

        private static IEnumerable<object> ParseJson(string json)
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                throw new ConnectorException(
                    "REST source must return a JSON array.",
                    new InvalidOperationException("Expected a JSON array at the root."),
                    "rest");

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
