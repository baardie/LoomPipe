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
    /// Reads HR / payroll data from Gusto using the v1 API.
    ///
    /// Parameters:
    ///   accessToken — Gusto OAuth bearer token
    ///   companyId   — the Gusto company UUID
    ///   resource    — employees, companies, payrolls, pay_periods, benefits, compensations,
    ///                 departments, locations, time_off_policies, contractors, contractor_payments
    /// </summary>
    public class GustoReader : ISourceReader
    {
        private const string BaseUrl = "https://api.gusto.com/v1";
        private const int DefaultPerPage = 100;

        private static readonly string[] AllResources =
        {
            "companies", "employees", "payrolls", "pay_periods", "benefits",
            "compensations", "departments", "locations", "time_off_policies",
            "contractors", "contractor_payments"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<GustoReader> _logger;

        public GustoReader(HttpClient httpClient, ILogger<GustoReader> logger)
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
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Gusto access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gusto");
            var companyId   = GetRequiredParam(config.Parameters, "companyId");

            _logger.LogInformation("Gusto: reading resource '{Resource}'.", resource);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, companyId);

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
                _logger.LogError(ex, "Gusto: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Gusto resource '{resource}': {ex.Message}", ex, "gusto");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Gusto access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gusto");
            var companyId   = GetRequiredParam(config.Parameters, "companyId");

            _logger.LogInformation("Gusto: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, companyId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Gusto: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Gusto schema for '{resource}': {ex.Message}", ex, "gusto");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Gusto access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "gusto");
            var companyId   = GetRequiredParam(config.Parameters, "companyId");

            _logger.LogInformation("Gusto: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, companyId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Gusto: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Gusto dry run preview failed for '{resource}': {ex.Message}", ex, "gusto");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string companyId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 1;

            do
            {
                var url = BuildListUrl(resource, companyId, page);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);

                int added = results.Count - countBefore;
                if (added == 0) break;

                // Non-paginated resources (companies, single objects) — break after first page.
                if (resource is "companies")
                    break;

                page++;
            }
            while (page <= maxPages);

            _logger.LogInformation("Gusto: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string companyId, int page)
        {
            var endpoint = resource switch
            {
                "companies"            => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}",
                "employees"            => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/employees",
                "payrolls"             => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/payrolls",
                "pay_periods"          => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/pay_periods",
                "benefits"             => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/benefits",
                "compensations"        => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/compensations",
                "departments"          => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/departments",
                "locations"            => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/locations",
                "time_off_policies"    => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/time_off_policies",
                "contractors"          => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/contractors",
                "contractor_payments"  => $"{BaseUrl}/companies/{Uri.EscapeDataString(companyId)}/contractor_payments",
                _ => throw new ConnectorException(
                    $"Unknown Gusto resource: '{resource}'.",
                    new ArgumentException($"Unknown resource: {resource}"),
                    "gusto")
            };

            // companies endpoint returns a single object, not paginated.
            if (resource == "companies")
                return endpoint;

            return $"{endpoint}?page={page}&per={DefaultPerPage}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in root.EnumerateArray())
                {
                    IDictionary<string, object> row = new ExpandoObject();
                    FlattenJsonObject(element, row);
                    results.Add(row);
                }
            }
            else if (root.ValueKind == JsonValueKind.Object)
            {
                // Single object response (e.g. companies).
                IDictionary<string, object> row = new ExpandoObject();
                FlattenJsonObject(root, row);
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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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
                    $"Gusto connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "gusto");
            return value;
        }
    }
}
