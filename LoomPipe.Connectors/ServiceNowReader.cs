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
    /// <summary>
    /// Reads records from ServiceNow using the Table API.
    ///
    /// Parameters:
    ///   accessToken  — OAuth Bearer token (or use username+password for Basic auth)
    ///   username     — ServiceNow username (Basic auth, alternative to accessToken)
    ///   password     — ServiceNow password (Basic auth, alternative to accessToken)
    ///   instance     — ServiceNow instance name (e.g. "mycompany" for mycompany.service-now.com)
    ///   resource     — table name: incident, problem, change_request, sys_user, cmdb_ci, etc.
    ///   query        — optional encoded query string for sysparm_query
    ///
    /// ConnectionString JSON: {"instance":"...","username":"...","password":"..."}
    /// </summary>
    public class ServiceNowReader : ISourceReader
    {
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "incident", "problem", "change_request", "sys_user", "cmdb_ci",
            "kb_knowledge", "sc_req_item", "task", "sys_user_group", "cmn_location"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ServiceNowReader> _logger;

        public ServiceNowReader(HttpClient httpClient, ILogger<ServiceNowReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var instance = ResolveInstance(config);
            var query = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("ServiceNow: reading resource '{Resource}' from instance '{Instance}'.", resource, instance);

            try
            {
                // Build watermark query filter if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    var wmQuery = $"{watermarkField}>{watermarkValue}";
                    query = string.IsNullOrEmpty(query) ? wmQuery : $"{query}^{wmQuery}";
                }

                return await ReadPaginatedAsync(config, instance, resource, query);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ServiceNow: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read ServiceNow resource '{resource}': {ex.Message}", ex, "servicenow");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var instance = ResolveInstance(config);

            _logger.LogInformation("ServiceNow: discovering schema for '{Resource}'.", resource);

            try
            {
                var records = await ReadPaginatedAsync(config, instance, resource, query: null, maxPages: 1);
                var first = records.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ServiceNow: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover ServiceNow schema for '{resource}': {ex.Message}", ex, "servicenow");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var instance = ResolveInstance(config);

            _logger.LogInformation("ServiceNow: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadPaginatedAsync(config, instance, resource, query: null, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ServiceNow: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"ServiceNow dry run preview failed for '{resource}': {ex.Message}", ex, "servicenow");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            DataSourceConfig config, string instance, string resource,
            string? query, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            do
            {
                var url = BuildUrl(instance, resource, query, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, config);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                page++;
                offset += PageLimit;

                if (fetched < PageLimit) break;
            }
            while (page < maxPages);

            _logger.LogInformation("ServiceNow: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string instance, string resource, string? query, int offset)
        {
            var sb = new StringBuilder($"https://{instance}.service-now.com/api/now/table/{resource}");
            sb.Append($"?sysparm_limit={PageLimit}&sysparm_offset={offset}");

            if (!string.IsNullOrEmpty(query))
            {
                sb.Append($"&sysparm_query={Uri.EscapeDataString(query)}");
            }

            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("result", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard ServiceNow response shape
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                results.Add(row);
            }
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String  => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number  => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True    => true,
                JsonValueKind.False   => false,
                JsonValueKind.Null    => string.Empty,
                _                     => value.ToString()
            };
        }

        // ── Auth helpers ─────────────────────────────────────────────────────

        private void ApplyAuth(HttpRequestMessage request, DataSourceConfig config)
        {
            var accessToken = GetStringParam(config.Parameters, "accessToken");

            if (!string.IsNullOrEmpty(accessToken))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                return;
            }

            // Try username+password from parameters.
            var username = GetStringParam(config.Parameters, "username");
            var password = GetStringParam(config.Parameters, "password");

            // Fall back to connectionString JSON.
            if (string.IsNullOrEmpty(username) && !string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var connDoc = JsonDocument.Parse(config.ConnectionString);
                    var root = connDoc.RootElement;
                    if (root.TryGetProperty("username", out var u)) username = u.GetString();
                    if (root.TryGetProperty("password", out var p)) password = p.GetString();
                }
                catch
                {
                    // ConnectionString is not JSON — treat as Bearer token.
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", config.ConnectionString);
                    return;
                }
            }

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", encoded);
                return;
            }

            throw new ConnectorException(
                "ServiceNow authentication is required. Provide 'accessToken', or 'username'+'password' via parameters or connection string JSON.",
                new ArgumentException("Missing authentication credentials."),
                "servicenow");
        }

        private string ResolveInstance(DataSourceConfig config)
        {
            var instance = GetStringParam(config.Parameters, "instance");

            if (!string.IsNullOrEmpty(instance)) return instance;

            // Try connectionString JSON.
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    if (doc.RootElement.TryGetProperty("instance", out var inst))
                        return inst.GetString()
                            ?? throw new ConnectorException(
                                "ServiceNow 'instance' is required.",
                                new ArgumentException("Missing 'instance'."),
                                "servicenow");
                }
                catch (JsonException) { /* not JSON */ }
            }

            throw new ConnectorException(
                "ServiceNow 'instance' parameter is required (e.g. 'mycompany' for mycompany.service-now.com).",
                new ArgumentException("Missing 'instance'."),
                "servicenow");
        }

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> p, string key)
        {
            var value = GetStringParam(p, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"ServiceNow connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "servicenow");
            return value;
        }
    }
}
