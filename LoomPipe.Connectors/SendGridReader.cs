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
    /// Reads data from the SendGrid v3 API.
    ///
    /// Parameters:
    ///   accessToken  — SendGrid API key
    ///   resource     — contacts, lists, segments, campaigns, stats, templates,
    ///                  suppressions, bounces, blocks, spam_reports
    ///   startDate    — optional ISO date for stats/time-based queries
    ///   endDate      — optional ISO date for stats/time-based queries
    /// </summary>
    public class SendGridReader : ISourceReader
    {
        private const string BaseUrl = "https://api.sendgrid.com/v3";

        private static readonly string[] AllResources =
        {
            "contacts", "lists", "segments", "campaigns", "stats",
            "templates", "suppressions", "bounces", "blocks", "spam_reports"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SendGridReader> _logger;

        public SendGridReader(HttpClient httpClient, ILogger<SendGridReader> logger)
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
                    "SendGrid API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sendgrid");

            _logger.LogInformation("SendGrid: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, config.Parameters);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SendGrid: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read SendGrid resource '{resource}': {ex.Message}", ex, "sendgrid");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "SendGrid API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sendgrid");

            _logger.LogInformation("SendGrid: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, config.Parameters, maxRecords: 10);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SendGrid: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover SendGrid schema for '{resource}': {ex.Message}", ex, "sendgrid");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "SendGrid API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "sendgrid");

            _logger.LogInformation("SendGrid: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, config.Parameters, maxRecords: sampleSize);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SendGrid: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"SendGrid dry run preview failed for '{resource}': {ex.Message}", ex, "sendgrid");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read ────────────────────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, Dictionary<string, object> parameters, int maxRecords = int.MaxValue)
        {
            var results = new List<object>();
            var startDate = GetStringParam(parameters, "startDate");
            var endDate   = GetStringParam(parameters, "endDate");

            var url = BuildUrl(resource, startDate, endDate);

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            ParseResponse(doc.RootElement, resource, results, maxRecords);

            _logger.LogInformation("SendGrid: read {Count} records from '{Resource}'.", results.Count, resource);
            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? startDate, string? endDate)
        {
            return resource switch
            {
                "contacts"     => $"{BaseUrl}/marketing/contacts",
                "lists"        => $"{BaseUrl}/marketing/lists",
                "segments"     => $"{BaseUrl}/marketing/segments",
                "campaigns"    => $"{BaseUrl}/marketing/singlesends",
                "stats"        => BuildStatsUrl(startDate, endDate),
                "templates"    => $"{BaseUrl}/templates?generations=dynamic",
                "suppressions" => $"{BaseUrl}/suppression/unsubscribes",
                "bounces"      => $"{BaseUrl}/suppression/bounces",
                "blocks"       => $"{BaseUrl}/suppression/blocks",
                "spam_reports" => $"{BaseUrl}/suppression/spam_reports",
                _              => throw new ConnectorException(
                    $"SendGrid: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "sendgrid")
            };
        }

        private static string BuildStatsUrl(string? startDate, string? endDate)
        {
            var sb = new StringBuilder($"{BaseUrl}/stats");
            var start = startDate ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
            sb.Append($"?start_date={Uri.EscapeDataString(start)}");
            if (!string.IsNullOrEmpty(endDate))
                sb.Append($"&end_date={Uri.EscapeDataString(endDate)}");
            return sb.ToString();
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResponse(JsonElement root, string resource, List<object> results, int maxRecords)
        {
            JsonElement items;

            // contacts: { "result": [...] } or { "contact_count": N, "result": [...] }
            if (resource == "contacts" && root.TryGetProperty("result", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // use result array
            }
            else if (root.TryGetProperty("results", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // standard results array (lists, segments, campaigns)
            }
            else if (root.TryGetProperty("result", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // alternative result array
            }
            else if (root.TryGetProperty("templates", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // templates response
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                // Single-object response — wrap it.
                IDictionary<string, object> singleRow = new ExpandoObject();
                foreach (var prop in root.EnumerateObject())
                {
                    singleRow[prop.Name] = ConvertJsonValue(prop.Value);
                }
                results.Add(singleRow);
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                if (results.Count >= maxRecords) break;

                IDictionary<string, object> row = new ExpandoObject();

                // For stats, flatten the nested "metrics" object.
                if (resource == "stats" && element.TryGetProperty("stats", out var statsArr)
                    && statsArr.ValueKind == JsonValueKind.Array)
                {
                    if (element.TryGetProperty("date", out var dateEl))
                        row["date"] = ConvertJsonValue(dateEl);

                    foreach (var statEntry in statsArr.EnumerateArray())
                    {
                        if (statEntry.TryGetProperty("metrics", out var metrics)
                            && metrics.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var metric in metrics.EnumerateObject())
                            {
                                row[metric.Name] = ConvertJsonValue(metric.Value);
                            }
                        }
                    }
                }
                else
                {
                    foreach (var prop in element.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Object || prop.Value.ValueKind == JsonValueKind.Array)
                            row[prop.Name] = prop.Value.ToString();
                        else
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
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
                    $"SendGrid connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "sendgrid");
            return value;
        }
    }
}
