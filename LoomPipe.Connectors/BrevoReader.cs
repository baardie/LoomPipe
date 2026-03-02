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
    /// Reads data from Brevo (formerly Sendinblue) using the v3 API.
    ///
    /// Parameters:
    ///   accessToken  — Brevo API key
    ///   resource     — contacts, lists, campaigns, transactional_emails, sms, templates,
    ///                  senders, webhooks, events
    /// </summary>
    public class BrevoReader : ISourceReader
    {
        private const string BaseUrl = "https://api.brevo.com/v3";
        private const int PageLimit = 50;

        private static readonly string[] AllResources =
        {
            "contacts", "lists", "campaigns", "transactional_emails", "sms",
            "templates", "senders", "webhooks", "events"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<BrevoReader> _logger;

        public BrevoReader(HttpClient httpClient, ILogger<BrevoReader> logger)
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
                    "Brevo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "brevo");

            _logger.LogInformation("Brevo: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Brevo: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Brevo resource '{resource}': {ex.Message}", ex, "brevo");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Brevo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "brevo");

            _logger.LogInformation("Brevo: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Brevo: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Brevo schema for '{resource}': {ex.Message}", ex, "brevo");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Brevo API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "brevo");

            _logger.LogInformation("Brevo: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Brevo: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Brevo dry run preview failed for '{resource}': {ex.Message}", ex, "brevo");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int offset = 0;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, offset);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, resource, results);
                int fetched = results.Count - countBefore;

                if (fetched == 0) break;

                offset += fetched;
                page++;
            }
            while (page < maxPages);

            _logger.LogInformation("Brevo: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, int offset)
        {
            string endpoint = resource switch
            {
                "contacts"              => $"{BaseUrl}/contacts",
                "lists"                 => $"{BaseUrl}/contacts/lists",
                "campaigns"             => $"{BaseUrl}/emailCampaigns",
                "transactional_emails"  => $"{BaseUrl}/smtp/statistics/events",
                "sms"                   => $"{BaseUrl}/smsCampaigns",
                "templates"             => $"{BaseUrl}/smtp/templates",
                "senders"               => $"{BaseUrl}/senders",
                "webhooks"              => $"{BaseUrl}/webhooks",
                "events"                => $"{BaseUrl}/events",
                _                       => $"{BaseUrl}/{resource}"
            };

            return $"{endpoint}?limit={PageLimit}&offset={offset}";
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resource, List<object> results)
        {
            // Brevo wraps results in a resource-specific key or returns arrays directly.
            string dataKey = resource switch
            {
                "contacts"              => "contacts",
                "lists"                 => "lists",
                "campaigns"             => "campaigns",
                "transactional_emails"  => "events",
                "sms"                   => "campaigns",
                "templates"             => "templates",
                "senders"               => "senders",
                "webhooks"              => "webhooks",
                "events"                => "events",
                _                       => resource
            };

            JsonElement items;

            if (root.TryGetProperty(dataKey, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard wrapped response
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

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Add("api-key", accessToken);
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
                    $"Brevo connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "brevo");
            return value;
        }
    }
}
