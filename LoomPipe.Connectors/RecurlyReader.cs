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
    /// Reads data from Recurly using the v3 REST API.
    ///
    /// Parameters:
    ///   accessToken  — Recurly API key (used for Basic auth)
    ///   resource     — accounts, subscriptions, invoices, transactions, plans, add_ons,
    ///                  coupons, items, shipping_methods, credit_payments
    /// </summary>
    public class RecurlyReader : ISourceReader
    {
        private const string BaseUrl = "https://v3.recurly.com";
        private const int PageLimit = 200;

        private static readonly string[] AllResources =
        {
            "accounts", "subscriptions", "invoices", "transactions", "plans",
            "add_ons", "coupons", "items", "shipping_methods", "credit_payments"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<RecurlyReader> _logger;

        public RecurlyReader(HttpClient httpClient, ILogger<RecurlyReader> logger)
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
                    "Recurly API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "recurly");

            _logger.LogInformation("Recurly: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Recurly: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Recurly resource '{resource}': {ex.Message}", ex, "recurly");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Recurly API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "recurly");

            _logger.LogInformation("Recurly: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Recurly: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Recurly schema for '{resource}': {ex.Message}", ex, "recurly");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Recurly API key is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "recurly");

            _logger.LogInformation("Recurly: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Recurly: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Recurly dry run preview failed for '{resource}': {ex.Message}", ex, "recurly");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based pagination via Link header) ──────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextUrl = $"{BaseUrl}/{resource}?limit={PageLimit}";
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, nextUrl);
                ApplyAuth(request, accessToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int countBefore = results.Count;
                ParseResultsPage(doc.RootElement, results);
                int fetched = results.Count - countBefore;

                if (fetched == 0) break;

                // Cursor-based pagination via Link header rel="next"
                nextUrl = ParseNextLink(response);

                page++;
            }
            while (nextUrl != null && page < maxPages);

            _logger.LogInformation("Recurly: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Link header parser ───────────────────────────────────────────────

        private static string? ParseNextLink(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return null;

            foreach (var linkHeader in linkValues)
            {
                // Link header format: <url>; rel="next", <url>; rel="prev"
                var parts = linkHeader.Split(',');
                foreach (var part in parts)
                {
                    var trimmed = part.Trim();
                    if (trimmed.Contains("rel=\"next\"", StringComparison.OrdinalIgnoreCase))
                    {
                        var urlStart = trimmed.IndexOf('<');
                        var urlEnd = trimmed.IndexOf('>');
                        if (urlStart >= 0 && urlEnd > urlStart)
                        {
                            return trimmed.Substring(urlStart + 1, urlEnd - urlStart - 1);
                        }
                    }
                }
            }

            return null;
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty("data", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Recurly v3 response shape
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
            // Recurly v3 uses Basic auth with the API key as username and empty password
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{accessToken}:"));
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
                    $"Recurly connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "recurly");
            return value;
        }
    }
}
