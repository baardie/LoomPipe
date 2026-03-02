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
    /// Reads HR data from BambooHR using their REST API.
    ///
    /// Parameters:
    ///   accessToken    — BambooHR API key
    ///   companyDomain  — your BambooHR subdomain (e.g. "mycompany")
    ///   resource       — employees, time_off, reports, tables, files, goals, training, benefit_plans
    /// </summary>
    public class BambooHRReader : ISourceReader
    {
        private const string BaseUrlTemplate = "https://api.bamboohr.com/api/gateway.php/{0}/v1";

        private static readonly string[] AllResources =
        {
            "employees", "time_off", "reports", "tables", "files",
            "goals", "training", "benefit_plans"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<BambooHRReader> _logger;

        public BambooHRReader(HttpClient httpClient, ILogger<BambooHRReader> logger)
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
            var resource      = GetRequiredParam(config.Parameters, "resource");
            var accessToken   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "BambooHR API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bamboohr");
            var companyDomain = GetRequiredParam(config.Parameters, "companyDomain");

            _logger.LogInformation("BambooHR: reading resource '{Resource}'.", resource);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, companyDomain);

                // Client-side watermark filtering if provided.
                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    records = records
                        .Where(r =>
                        {
                            var dict = r as IDictionary<string, object>;
                            if (dict == null || !dict.TryGetValue(watermarkField, out var val)) return false;
                            return string.Compare(val?.ToString(), watermarkValue, StringComparison.OrdinalIgnoreCase) > 0;
                        })
                        .ToList();
                }

                return records;
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BambooHR: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read BambooHR resource '{resource}': {ex.Message}", ex, "bamboohr");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource      = GetRequiredParam(config.Parameters, "resource");
            var accessToken   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "BambooHR API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bamboohr");
            var companyDomain = GetRequiredParam(config.Parameters, "companyDomain");

            _logger.LogInformation("BambooHR: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, companyDomain);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BambooHR: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover BambooHR schema for '{resource}': {ex.Message}", ex, "bamboohr");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource      = GetRequiredParam(config.Parameters, "resource");
            var accessToken   = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "BambooHR API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "bamboohr");
            var companyDomain = GetRequiredParam(config.Parameters, "companyDomain");

            _logger.LogInformation("BambooHR: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, companyDomain);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BambooHR: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"BambooHR dry run preview failed for '{resource}': {ex.Message}", ex, "bamboohr");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read ────────────────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string companyDomain)
        {
            var baseUrl = string.Format(BaseUrlTemplate, Uri.EscapeDataString(companyDomain));
            var results = new List<object>();

            var url = resource switch
            {
                "employees"     => $"{baseUrl}/employees/directory",
                "time_off"      => $"{baseUrl}/time_off/requests/?status=approved",
                "reports"       => $"{baseUrl}/reports/custom?format=JSON",
                "tables"        => $"{baseUrl}/employees/0/tables/customTables",
                "files"         => $"{baseUrl}/employees/0/files/view",
                "goals"         => $"{baseUrl}/performance/employees/0/goals",
                "training"      => $"{baseUrl}/training/record/employee/all",
                "benefit_plans" => $"{baseUrl}/benefitplans",
                _               => throw new ConnectorException(
                    $"Unknown BambooHR resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "bamboohr")
            };

            if (resource == "reports")
            {
                // Custom report requires a POST with field list.
                var body = JsonSerializer.Serialize(new
                {
                    fields = new[]
                    {
                        "firstName", "lastName", "department", "jobTitle",
                        "workEmail", "workPhone", "hireDate", "status"
                    }
                });

                using var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new StringContent(body, Encoding.UTF8, "application/json")
                };
                ApplyAuth(request, accessToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                ParseResultsArray(doc.RootElement, "employees", results);
            }
            else
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // The employees directory returns { "employees": [...] }; other endpoints return arrays or objects.
                if (resource == "employees"
                    && doc.RootElement.TryGetProperty("employees", out var emps)
                    && emps.ValueKind == JsonValueKind.Array)
                {
                    ParseJsonArray(emps, results);
                }
                else
                {
                    ParseResultsArray(doc.RootElement, resource, results);
                }
            }

            _logger.LogInformation("BambooHR: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsArray(JsonElement root, string arrayProp, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty(arrayProp, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Found named array
            }
            else if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard "results" wrapper
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                // Single object — wrap it
                IDictionary<string, object> singleRow = new ExpandoObject();
                FlattenJsonObject(root, singleRow);
                results.Add(singleRow);
                return;
            }

            ParseJsonArray(items, results);
        }

        private static void ParseJsonArray(JsonElement items, List<object> results)
        {
            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(element, row);
                results.Add(row);
            }
        }

        private static void FlattenJsonObject(JsonElement element, IDictionary<string, object> row)
        {
            if (element.ValueKind != JsonValueKind.Object) return;

            foreach (var prop in element.EnumerateObject())
            {
                row[prop.Name] = ConvertJsonValue(prop.Value);
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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string apiKey)
        {
            // BambooHR uses Basic auth with apiKey:x
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:x"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
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
                    $"BambooHR connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "bamboohr");
            return value;
        }
    }
}
